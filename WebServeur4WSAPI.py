#!/usr/bin/env python3
"""
WebServeur4WSAPI
Minimal Web API server for a web client.

Run:
    python WebServeur4WSAPI.py
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import queue
import subprocess
import sys
import time
import webbrowser
from threading import Lock
from urllib.parse import urlparse


HOST = "127.0.0.1"
PORT = 8000
BASE_DIR = Path(__file__).resolve().parent
ROBOT_LOGIN_RESULT_PRM = BASE_DIR / "robot_login_result.prm"
PARAMETERS_PRM = BASE_DIR / "WebServeurParameters.prm"
LOG_FILE = BASE_DIR / "log.json"
LOG_MAX_LINES = 1000
TRAJECTORY_DIR = BASE_DIR / "InitTrajFiles"
ROBOT_LOGIN_API_URL = "http://192.168.100.100/api/v4/access-service/login"
ROBOT_LOGOUT_API_URL = "http://192.168.100.100/api/v4/access-service/logout"
COMMAND_QUEUE: queue.Queue = queue.Queue()
COMMAND_RESULTS = {}
COMMAND_COUNTER = 0
# Maps command_id → Queue used to hand result back to the blocking POST handler.
PENDING_RESULTS: dict[int, queue.Queue] = {}
ROBOT_LIST: list = []
ROBOT_STATUS_DATA: dict = {}  # name → latest robot_status data
ROBOT_AXES_DATA: dict = {}    # name → latest act_values_axes data
ROBOT_TCP_DATA: dict = {}     # name → latest act_values_tcp data
VARIABLE_DATA: list = []     # latest variable-service publish_data payload
SERVER_STARTED_AT = datetime.now(timezone.utc).isoformat()
ROBOT_LOGIN_STATUS = {
    "state": "unknown",
    "last_command_id": None,
    "message": "No login attempt yet",
    "last_refresh_at": None,
    "last_refresh_ok": None,
    "access_token": None,
}
STORE_LOCK = Lock()
_log_lock = Lock()


def _stop_existing_python_script_instances(script_path: Path, service_label: str) -> None:
    """Stop old python processes that run the same script in this workspace."""
    my_pid = os.getpid()
    parent_pid = os.getppid()
    script_marker = str(script_path).lower().replace("\\", "/")
    script_name = script_path.name.lower()

    ps_query = (
        "$ErrorActionPreference='Stop';"
        "Get-CimInstance Win32_Process | "
        f"Where-Object {{ $_.CommandLine -and $_.CommandLine.ToLower().Contains('{script_name}') }} | "
        "Select-Object ProcessId, CommandLine | ConvertTo-Json -Compress"
    )

    try:
        raw = subprocess.check_output(
            ["powershell", "-NoProfile", "-Command", ps_query],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return

    if not raw:
        return

    try:
        parsed = json.loads(raw)
    except Exception:
        return

    processes = parsed if isinstance(parsed, list) else [parsed]
    for proc in processes:
        if not isinstance(proc, dict):
            continue

        pid = proc.get("ProcessId")
        cmdline = str(proc.get("CommandLine", "")).lower().replace("\\", "/")
        if not isinstance(pid, int):
            continue
        if pid == my_pid:
            continue
        if pid == parent_pid:
            continue
        if script_marker not in cmdline:
            continue

        try:
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
            print(f"[WebServeur4WSAPI] Stopped previous {service_label} (pid={pid})")
        except Exception:
            continue


def _stop_existing_wsapi_server_instances() -> None:
    """Ensure only one WebServeur4WSAPI process is active for this workspace."""
    _stop_existing_python_script_instances(Path(__file__).resolve(), "WebServeur4WSAPI server")


def _stop_existing_wsapi_client_instances(client_script: Path) -> None:
    """Ensure only one WSAPIClient process is active for this workspace."""
    _stop_existing_python_script_instances(client_script, "WSAPI client")


def _start_wsapi_client_process() -> subprocess.Popen | None:
    client_script = BASE_DIR / "WSAPIClient.py"
    if not client_script.exists():
        print("[WebServeur4WSAPI] WSAPIClient.py not found, auto-start skipped")
        return None

    _stop_existing_wsapi_client_instances(client_script)

    # Silence WSAPIClient output — only server logs appear in the terminal.
    process = subprocess.Popen(
        [sys.executable, str(client_script)],
        cwd=str(BASE_DIR),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    print(f"[WebServeur4WSAPI] WSAPI client started (pid={process.pid})")
    return process


def _stop_wsapi_client_process(process: subprocess.Popen | None) -> None:
    if process is None:
        return

    if process.poll() is not None:
        return

    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)

    print("[WebServeur4WSAPI] WSAPI client stopped")


def _read_parameters() -> dict:
    """Parse WebServeurParameters.prm into a key-value dict."""
    if not PARAMETERS_PRM.exists():
        return {}
    result = {}
    for line in PARAMETERS_PRM.read_text(encoding="utf-8").splitlines():
        idx = line.find("=")
        if idx > 0:
            result[line[:idx].strip()] = line[idx + 1:].strip()
    return result


def _next_command_id() -> int:
    global COMMAND_COUNTER
    with STORE_LOCK:
        COMMAND_COUNTER += 1
        return COMMAND_COUNTER


def _normalize_login_result_for_prm(result: dict) -> tuple[int, str, dict]:
    raw_status = result.get("status_code", 0)
    try:
        raw_status_int = int(raw_status)
    except (TypeError, ValueError):
        raw_status_int = 0

    response_raw = result.get("response", {})

    if raw_status_int == 200:
        normalized_code = 200
        code_description = "successful operation"
        if isinstance(response_raw, dict):
            response_json = response_raw
        else:
            response_json = {
                "refresh_token": "",
                "access_token": "",
                "token_type": "bearer",
                "password_expired": False,
                "inactivity_timeout": 0,
            }
        return normalized_code, code_description, response_json

    if 400 <= raw_status_int < 500:
        normalized_code = 400
        code_description = "invalid path or invalid parameters"
    else:
        normalized_code = 500
        code_description = "internal service error, e.g. internal function call failed"

    if isinstance(response_raw, dict) and isinstance(response_raw.get("error"), dict):
        response_json = response_raw
    else:
        response_json = {
            "error": {
                "key": "robot_login_error",
                "msg": str(response_raw),
            }
        }

    return normalized_code, code_description, response_json


def _save_robot_login_result_prm(status: dict, payload: dict) -> None:
    result = payload.get("result", {})
    raw_status_code = result.get("status_code", "")
    normalized_code, code_description, normalized_response_json = _normalize_login_result_for_prm(result)
    response_json_pretty = json.dumps(normalized_response_json, ensure_ascii=True)

    lines = [
        f"updated_at_utc={datetime.now(timezone.utc).isoformat()}",
        f"type={payload.get('type', '')}",
        f"state={status.get('state', 'unknown')}",
        f"message={status.get('message', '')}",
        f"last_command_id={status.get('last_command_id', '')}",
        f"robot_url={payload.get('url', '')}",
        f"ok={result.get('ok', False)}",
        f"raw_status_code={raw_status_code}",
        f"status_code={normalized_code}",
        f"code_description={code_description}",
        f"response_json={response_json_pretty}",
    ]
    ROBOT_LOGIN_RESULT_PRM.write_text("\n".join(lines) + "\n", encoding="utf-8")


class WebApiHandler(BaseHTTPRequestHandler):
    def _send_json(self, status_code: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self) -> dict | None:
        content_length = int(self.headers.get("Content-Length", "0"))
        raw_data = self.rfile.read(content_length) if content_length > 0 else b"{}"
        try:
            return json.loads(raw_data.decode("utf-8")) if raw_data else {}
        except json.JSONDecodeError:
            return None

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        path = urlparse(self.path).path

        if path == "/":
            html_path = BASE_DIR / "index.html"
            if not html_path.exists():
                self._send_json(500, {"error": "Missing index.html"})
                return

            body = html_path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)
            return

        # Serve static files under /Images/
        _STATIC_TYPES = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
                         ".gif": "image/gif", ".svg": "image/svg+xml", ".ico": "image/x-icon",
                         ".webp": "image/webp", ".css": "text/css", ".js": "application/javascript"}
        if path.startswith("/Images/") or path.startswith("/images/"):
            file_path = BASE_DIR / path.lstrip("/")
            if file_path.exists() and file_path.is_file():
                body = file_path.read_bytes()
                mime = _STATIC_TYPES.get(file_path.suffix.lower(), "application/octet-stream")
                self.send_response(200)
                self.send_header("Content-Type", mime)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "max-age=3600")
                self.end_headers()
                self.wfile.write(body)
                return
            self._send_json(404, {"error": "Image not found", "path": path})
            return

        if path == "/health":
            self._send_json(200, {"status": "ok", "service": "WebServeur4WSAPI", "started_at": SERVER_STARTED_AT})
            return

        if path == "/api/time":
            now = datetime.now(timezone.utc).isoformat()
            self._send_json(200, {"utc_time": now})
            return

        if path == "/api/command":
            # Block until a command is queued (long-poll), timeout after 25 s.
            try:
                command_item = COMMAND_QUEUE.get(block=True, timeout=25)
                self._send_json(200, command_item)
            except queue.Empty:
                self._send_json(200, {"status": "no_command"})
            return

        if path == "/api/command-results":
            with STORE_LOCK:
                snapshot = dict(COMMAND_RESULTS)
            self._send_json(200, {"results": snapshot})
            return

        if path == "/api/robot-login-status":
            with STORE_LOCK:
                status_snapshot = dict(ROBOT_LOGIN_STATUS)
            self._send_json(200, status_snapshot)
            return

        if path == "/api/parameters":
            content = PARAMETERS_PRM.read_text(encoding="utf-8") if PARAMETERS_PRM.exists() else ""
            self._send_json(200, {"prm_file": PARAMETERS_PRM.name, "content": content})
            return

        if path == "/api/robot-list":
            with STORE_LOCK:
                snapshot = list(ROBOT_LIST)
            self._send_json(200, {"robots": snapshot})
            return

        if path == "/api/robot-status":
            with STORE_LOCK:
                snapshot = dict(ROBOT_STATUS_DATA)
            self._send_json(200, snapshot)
            return

        if path == "/api/robot-axes":
            with STORE_LOCK:
                snapshot = dict(ROBOT_AXES_DATA)
            self._send_json(200, snapshot)
            return

        if path == "/api/robot-tcp":
            with STORE_LOCK:
                snapshot = dict(ROBOT_TCP_DATA)
            self._send_json(200, snapshot)
            return

        if path == "/api/variables":
            with STORE_LOCK:
                snapshot = list(VARIABLE_DATA)
            self._send_json(200, {"data": snapshot})
            return

        if path == "/api/trajectory-files":
            if TRAJECTORY_DIR.exists() and TRAJECTORY_DIR.is_dir():
                files = sorted([
                    p.name for p in TRAJECTORY_DIR.iterdir()
                    if p.is_file() and p.suffix.lower() == ".csv"
                ])
            else:
                files = []
            self._send_json(200, {"files": files})
            return

        self._send_json(404, {"error": "Not found", "path": path})

    def do_POST(self):
        raw_path = urlparse(self.path).path
        path = raw_path.rstrip("/") or "/"

        if path == "/api/log-entry":
            payload = self._read_json_body() or {}
            msg = str(payload.get("message", "")).strip()
            if msg:
                with _log_lock:
                    try:
                        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                        line = json.dumps({
                            "ts": ts,
                            "event": "server_log",
                            "robot": None,
                            "payload": {"message": msg},
                        }, ensure_ascii=True) + "\n"
                        existing = LOG_FILE.read_text(encoding="utf-8").splitlines(keepends=True) if LOG_FILE.exists() else []
                        lines = (existing + [line])[-LOG_MAX_LINES:]
                        LOG_FILE.write_text("".join(lines), encoding="utf-8")
                    except Exception:
                        pass
            self._send_json(200, {"status": "ok"})
            return

        if path == "/api/robot-list-notify":
            payload = self._read_json_body() or {}
            with STORE_LOCK:
                ROBOT_LIST.clear()
                ROBOT_LIST.extend(payload.get("robots", []))
                if not ROBOT_LIST:
                    ROBOT_STATUS_DATA.clear()
            self._send_json(200, {"status": "ok"})
            return

        if path == "/api/robot-status-notify":
            payload = self._read_json_body() or {}
            name = payload.get("robot", "")
            data = payload.get("data")
            if name and data is not None:
                with STORE_LOCK:
                    ROBOT_STATUS_DATA[name] = data
            self._send_json(200, {"status": "ok"})
            return

        if path == "/api/robot-axes-notify":
            payload = self._read_json_body() or {}
            name = payload.get("robot", "")
            data = payload.get("data")
            if name and data is not None:
                with STORE_LOCK:
                    ROBOT_AXES_DATA[name] = data
            self._send_json(200, {"status": "ok"})
            return

        if path == "/api/robot-tcp-notify":
            payload = self._read_json_body() or {}
            name = payload.get("robot", "")
            data = payload.get("data")
            if name and data is not None:
                with STORE_LOCK:
                    ROBOT_TCP_DATA[name] = data
            self._send_json(200, {"status": "ok"})
            return

        if path == "/api/variables-notify":
            payload = self._read_json_body() or {}
            data = payload.get("data")
            if isinstance(data, list):
                with STORE_LOCK:
                    VARIABLE_DATA[:] = data
            self._send_json(200, {"status": "ok"})
            return

        if path == "/api/parameters":
            payload = self._read_json_body()
            if payload is None:
                self._send_json(400, {"error": "Invalid JSON payload"})
                return
            lines = [f"updated_at_utc={datetime.now(timezone.utc).isoformat()}"]
            for key, value in payload.items():
                lines.append(f"{key}={value}")
            PARAMETERS_PRM.write_text("\n".join(lines) + "\n", encoding="utf-8")
            self._send_json(200, {"status": "saved", "prm_file": PARAMETERS_PRM.name})
            return

        if path == "/api/refresh-notify":
            payload_body = self._read_json_body() or {}
            ok = bool(payload_body.get("ok", True))
            token = payload_body.get("access_token", None)
            with STORE_LOCK:
                ROBOT_LOGIN_STATUS["last_refresh_ok"] = ok
                if ok:
                    ROBOT_LOGIN_STATUS["last_refresh_at"] = datetime.now(timezone.utc).isoformat()
                if token is not None:
                    ROBOT_LOGIN_STATUS["access_token"] = token if ok else None
            self._send_json(200, {"status": "ok"})
            return

        if path in ("/api/robot-login", "/api/robot_login"):
            payload = self._read_json_body()
            if payload is None:
                self._send_json(400, {"error": "Invalid JSON payload"})
                return

            prm = _read_parameters()
            robot_ip = prm.get("api_ip", "")
            robot_url = f"http://{robot_ip}/api/v4/access-service/login" if robot_ip else ROBOT_LOGIN_API_URL
            command_id = _next_command_id()
            command_item = {
                "id": command_id,
                "type": "robot_login",
                "url": robot_url,
                "username": prm.get("api_username", ""),
                "password": prm.get("api_password", ""),
            }

            result_q: queue.Queue = queue.Queue()
            with STORE_LOCK:
                PENDING_RESULTS[command_id] = result_q
                ROBOT_LOGIN_STATUS["state"] = "pending"
                ROBOT_LOGIN_STATUS["last_command_id"] = command_id
                ROBOT_LOGIN_STATUS["message"] = "Login en cours..."
            COMMAND_QUEUE.put(command_item)

            try:
                result_payload = result_q.get(timeout=35)
                result = result_payload.get("result", {})
                ok = bool(result.get("ok"))
                self._send_json(200 if ok else 502, {"ok": ok, "result": result})
            except queue.Empty:
                with STORE_LOCK:
                    PENDING_RESULTS.pop(command_id, None)
                    ROBOT_LOGIN_STATUS["state"] = "failure"
                    ROBOT_LOGIN_STATUS["message"] = "Timeout - no response from WSAPI client"
                self._send_json(504, {"error": "Timeout waiting for WSAPI result"})
            return

        if path in ("/api/robot-logout", "/api/robot_logout"):
            payload = self._read_json_body()
            if payload is None:
                self._send_json(400, {"error": "Invalid JSON payload"})
                return

            command_id = _next_command_id()
            command_item = {
                "id": command_id,
                "type": "robot_logout",
                "url": ROBOT_LOGOUT_API_URL,
            }

            result_q = queue.Queue()
            with STORE_LOCK:
                PENDING_RESULTS[command_id] = result_q
                ROBOT_LOGIN_STATUS["state"] = "pending"
                ROBOT_LOGIN_STATUS["last_command_id"] = command_id
                ROBOT_LOGIN_STATUS["message"] = "Logout en cours..."
            COMMAND_QUEUE.put(command_item)

            try:
                result_payload = result_q.get(timeout=35)
                result = result_payload.get("result", {})
                ok = bool(result.get("ok"))
                self._send_json(200 if ok else 502, {"ok": ok, "result": result})
            except queue.Empty:
                with STORE_LOCK:
                    PENDING_RESULTS.pop(command_id, None)
                    ROBOT_LOGIN_STATUS["state"] = "failure"
                    ROBOT_LOGIN_STATUS["message"] = "Timeout - no response from WSAPI client"
                self._send_json(504, {"error": "Timeout waiting for WSAPI result"})
            return

        if path == "/api/robot-traj":
            payload = self._read_json_body()
            if payload is None:
                self._send_json(400, {"error": "Invalid JSON payload"})
                return

            robot_name = str(payload.get("robot", "")).strip()
            trajectory_name = str(payload.get("trajectory", "")).strip()

            if not robot_name:
                self._send_json(400, {"error": "Missing robot"})
                return

            if not trajectory_name:
                self._send_json(400, {"error": "Missing trajectory"})
                return

            # Keep only file names to avoid path traversal.
            if Path(trajectory_name).name != trajectory_name:
                self._send_json(400, {"error": "Invalid trajectory file name"})
                return

            trajectory_path = TRAJECTORY_DIR / trajectory_name
            if not trajectory_path.exists() or not trajectory_path.is_file():
                self._send_json(404, {"error": "Trajectory file not found", "trajectory": trajectory_name})
                return

            with STORE_LOCK:
                tcp_snapshot = ROBOT_TCP_DATA.get(robot_name, {})

            command_id = _next_command_id()
            command_item = {
                "id": command_id,
                "type": "robot_traj",
                "robot": robot_name,
                "trajectory": trajectory_name,
                "current_tcp": tcp_snapshot,
            }

            result_q = queue.Queue()
            with STORE_LOCK:
                PENDING_RESULTS[command_id] = result_q
            COMMAND_QUEUE.put(command_item)

            try:
                result_payload = result_q.get(timeout=90)
                result = result_payload.get("result", {})
                ok = bool(result.get("ok"))
                self._send_json(200 if ok else 502, {"ok": ok, "result": result})
            except queue.Empty:
                with STORE_LOCK:
                    PENDING_RESULTS.pop(command_id, None)
                self._send_json(504, {"error": "Timeout waiting for WSAPITraj result"})
            return

        if path == "/api/robot-traj-start":
            payload = self._read_json_body()
            if payload is None:
                self._send_json(400, {"error": "Invalid JSON payload"})
                return

            robot_name = str(payload.get("robot", "")).strip()
            trajectory_name = str(payload.get("trajectory", "")).strip()

            if not robot_name:
                self._send_json(400, {"error": "Missing robot"})
                return
            if not trajectory_name:
                self._send_json(400, {"error": "Missing trajectory"})
                return

            with STORE_LOCK:
                status_snapshot = ROBOT_STATUS_DATA.get(robot_name, {})
            overlap_percent = 100.0
            if isinstance(status_snapshot, dict):
                raw_override = status_snapshot.get("override", 100)
                try:
                    overlap_percent = float(raw_override)
                except (TypeError, ValueError):
                    overlap_percent = 100.0
            overlap_percent = max(0.0, min(200.0, overlap_percent))

            command_id = _next_command_id()
            command_item = {
                "id": command_id,
                "type": "robot_traj_start",
                "robot": robot_name,
                "trajectory": trajectory_name,
                "overlap_percent": overlap_percent,
            }

            result_q = queue.Queue()
            with STORE_LOCK:
                PENDING_RESULTS[command_id] = result_q
            COMMAND_QUEUE.put(command_item)

            try:
                result_payload = result_q.get(timeout=180)
                result = result_payload.get("result", {})
                ok = bool(result.get("ok"))
                self._send_json(200 if ok else 502, {"ok": ok, "result": result})
            except queue.Empty:
                with STORE_LOCK:
                    PENDING_RESULTS.pop(command_id, None)
                self._send_json(504, {"error": "Timeout waiting for trajectory start result"})
            return

        if path == "/api/robot-cmd":
            payload = self._read_json_body()
            if payload is None:
                self._send_json(400, {"error": "Invalid JSON payload"})
                return

            robot_name = str(payload.get("robot", "")).strip()
            action = str(payload.get("action", "")).strip().lower()

            if not robot_name:
                self._send_json(400, {"error": "Missing robot"})
                return

            allowed_actions = {
                "power",
                "error_reset",
                "power_enable",
                "power_disable",
                "path_execution_start",
                "path_execution_stop",
            }
            if action not in allowed_actions:
                self._send_json(400, {"error": "Invalid action", "allowed_actions": sorted(list(allowed_actions))})
                return

            command_id = _next_command_id()
            command_item = {
                "id": command_id,
                "type": "robot_cmd",
                "robot": robot_name,
                "action": action,
            }

            result_q = queue.Queue()
            with STORE_LOCK:
                PENDING_RESULTS[command_id] = result_q
            COMMAND_QUEUE.put(command_item)

            try:
                result_payload = result_q.get(timeout=35)
                result = result_payload.get("result", {})
                ok = bool(result.get("ok"))
                self._send_json(200 if ok else 502, {"ok": ok, "result": result})
            except queue.Empty:
                with STORE_LOCK:
                    PENDING_RESULTS.pop(command_id, None)
                self._send_json(504, {"error": "Timeout waiting for robot cmd result"})
            return

        if path == "/api/variable-write":
            payload = self._read_json_body()
            if payload is None:
                self._send_json(400, {"error": "Invalid JSON payload"})
                return

            name = str(payload.get("name", "")).strip()
            if not name:
                self._send_json(400, {"error": "Missing variable name"})
                return

            command_id = _next_command_id()
            command_item = {
                "id": command_id,
                "type": "variable_write",
                "name": name,
                "value": payload.get("value"),
                "var_type": payload.get("type"),
            }

            result_q = queue.Queue()
            with STORE_LOCK:
                PENDING_RESULTS[command_id] = result_q
            COMMAND_QUEUE.put(command_item)

            try:
                result_payload = result_q.get(timeout=10)
                result = result_payload.get("result", {})
                ok = bool(result.get("ok"))
                self._send_json(200 if ok else 502, {"ok": ok, "result": result})
            except queue.Empty:
                with STORE_LOCK:
                    PENDING_RESULTS.pop(command_id, None)
                self._send_json(504, {"error": "Timeout waiting for variable write result"})
            return

        if path == "/api/command":
            payload = self._read_json_body()
            if payload is None:
                self._send_json(400, {"error": "Invalid JSON payload"})
                return

            command_text = str(payload.get("command", "")).strip()
            if not command_text:
                self._send_json(400, {"error": "Missing command"})
                return

            command_id = _next_command_id()
            command_item = {"id": command_id, "command": command_text}

            COMMAND_QUEUE.put(command_item)

            self._send_json(201, {"status": "queued", **command_item})
            return

        if path == "/api/command-result":
            payload = self._read_json_body()
            if payload is None:
                self._send_json(400, {"error": "Invalid JSON payload"})
                return

            command_id = payload.get("id")
            if command_id is None:
                self._send_json(400, {"error": "Missing command id"})
                return

            with STORE_LOCK:
                COMMAND_RESULTS[str(command_id)] = payload
                pending_q = PENDING_RESULTS.pop(command_id, None)
                result = payload.get("result", {})
                ok = bool(result.get("ok"))
                status_code = result.get("status_code", "")
                if payload.get("type") == "robot_login":
                    ROBOT_LOGIN_STATUS["state"] = "success" if ok else "failure"
                    ROBOT_LOGIN_STATUS["last_command_id"] = command_id
                    ROBOT_LOGIN_STATUS["message"] = (
                        f"Robot login succeeded (HTTP {status_code})"
                        if ok else f"Robot login failed (HTTP {status_code})"
                    )
                elif payload.get("type") == "robot_logout":
                    ROBOT_LOGIN_STATUS["state"] = "failure"
                    ROBOT_LOGIN_STATUS["last_command_id"] = command_id
                    ROBOT_LOGIN_STATUS["message"] = (
                        f"Robot logout succeeded (HTTP {status_code})"
                        if ok else f"Robot logout failed (HTTP {status_code})"
                    )

            if pending_q is not None:
                pending_q.put(payload)

            cmd_type = payload.get("type", "")
            if cmd_type in ("robot_login", "robot_logout"):
                print(f"[WebServeur4WSAPI] {cmd_type}: ok={ok} HTTP {status_code}")
            elif cmd_type == "robot_traj":
                print(f"[WebServeur4WSAPI] robot_traj: ok={ok}")
            elif cmd_type == "robot_traj_start":
                print(f"[WebServeur4WSAPI] robot_traj_start: ok={ok}")
            elif cmd_type == "robot_cmd":
                print(f"[WebServeur4WSAPI] robot_cmd: ok={ok}")

            self._send_json(200, {"status": "stored", "id": command_id})
            return

        if path != "/api/echo":
            self._send_json(404, {"error": "Not found", "path": path})
            return

        payload = self._read_json_body()
        if payload is None:
            self._send_json(400, {"error": "Invalid JSON payload"})
            return

        self._send_json(200, {"received": payload})

    def log_message(self, format_str: str, *args):
        # Silence routine WSAPI polling to keep logs readable.
        message = format_str % args
        if ('"GET /api/command HTTP' in message
                or '"GET /api/robot-login-status HTTP' in message
                or '"POST /api/command-result HTTP' in message
                or '"POST /api/robot-login HTTP' in message
                or '"POST /api/robot-logout HTTP' in message
                or '"POST /api/log-entry HTTP' in message
                or '"POST /api/robot-status-notify HTTP' in message
                or '"POST /api/robot-axes-notify HTTP' in message
                or '"POST /api/robot-tcp-notify HTTP' in message
                or '"POST /api/variables-notify HTTP' in message
                or '"GET /api/robot-status HTTP' in message
                or '"GET /api/robot-axes HTTP' in message
                or '"GET /api/robot-tcp HTTP' in message
                or '"GET /api/variables HTTP' in message
                or '"GET /api/trajectory-files HTTP' in message
                or '"POST /api/robot-traj HTTP' in message
                or '"POST /api/robot-traj-start HTTP' in message
                or '"POST /api/robot-cmd HTTP' in message):
            return
        print("[WebServeur4WSAPI]", message)


def run_server(host: str = HOST, port: int = PORT) -> None:
    _stop_existing_wsapi_server_instances()

    if not PARAMETERS_PRM.exists():
        PARAMETERS_PRM.write_text(
            "updated_at_utc=\n"
            "api_username=\n"
            "api_password=\n"
            "api_ip=\n",
            encoding="utf-8",
        )

    # Windows may still hold the listening socket briefly after taskkill.
    ThreadingHTTPServer.allow_reuse_address = True
    server = None
    last_error = None
    for attempt in range(20):
        try:
            server = ThreadingHTTPServer((host, port), WebApiHandler)
            break
        except OSError as exc:
            last_error = exc
            if attempt == 19:
                raise
            time.sleep(0.25)

    if server is None:
        raise RuntimeError(f"Unable to start server on {host}:{port}") from last_error

    wsapi_process = _start_wsapi_client_process()
    server_url = f"http://{host}:{port}"
    print(f"[WebServeur4WSAPI] running on {server_url}")
    webbrowser.open(server_url)
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[WebServeur4WSAPI] stopped")
    finally:
        server.server_close()
        _stop_wsapi_client_process(wsapi_process)


if __name__ == "__main__":
    run_server()
