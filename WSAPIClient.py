#!/usr/bin/env python3
"""
WSAPIClient
Polls WebServeur4WSAPI for commands, executes them, and sends results back.

Expected server endpoints:
- GET  /api/command
- POST /api/command-result

Run:
    python WSAPIClient.py
"""

import json
import importlib.util
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from urllib import error, request

_websocket_lib = None
try:
    import websocket as _websocket_lib  # type: ignore[reportMissingImports]
    _HAS_WEBSOCKET = True
except ImportError:
    _HAS_WEBSOCKET = False


SERVER_BASE_URL = "http://127.0.0.1:8000"
_ROBOT_URL_PATH_LOGIN = "/api/v4/access-service/login"
_ROBOT_URL_PATH_LOGOUT = "/api/v4/access-service/logout"
_ROBOT_URL_PATH_REFRESH = "/api/v4/access-service/refresh"
_ROBOT_URL_PATH_ROBOTS = "/api/v4/robot-control/robots"
_ROBOT_URL_PATH_WS = "/api/v4/robot-control/robots/{name}/websocket-subscribe"
_ROBOT_URL_PATH_WS_CMD = "/api/v4/robot-control/robots/{name}/websocket-command"
PARAMETERS_PRM = Path(__file__).resolve().parent / "WebServeurParameters.prm"
REQUEST_TIMEOUT_SECONDS = 30  # must exceed server long-poll timeout (25 s)
COMMAND_TIMEOUT_SECONDS = 30
ROBOT_LOGIN_TIMEOUT_SECONDS = 10
LAST_ACCESS_TOKEN = ""
LAST_REFRESH_TOKEN = ""
SESSION_FILE = Path(__file__).resolve().parent / "session.prm"
_REFRESH_INTERVAL = 5
_refresh_timer: threading.Timer | None = None
_session_restored = False
_last_refresh_ok = True  # assume OK until proven otherwise
LOG_FILE = Path(__file__).resolve().parent / "log.json"
LOG_MAX_LINES = 1000
TRAJ_SCRIPT = Path(__file__).resolve().parent / "WSAPITraj.py"
_TRAJ_MODULE = None
_TRAJ_MODULE_MTIME_NS: int | None = None
_log_lock = threading.Lock()
_ws_client_id_counter = 0
_ws_client_id_lock = threading.Lock()


def _get_traj_module():
    global _TRAJ_MODULE, _TRAJ_MODULE_MTIME_NS
    if not TRAJ_SCRIPT.exists():
        return None
    source_mtime_ns = TRAJ_SCRIPT.stat().st_mtime_ns
    if _TRAJ_MODULE is not None and _TRAJ_MODULE_MTIME_NS == source_mtime_ns:
        return _TRAJ_MODULE
    try:
        spec = importlib.util.spec_from_file_location("wsapi_traj_module", str(TRAJ_SCRIPT))
        if spec is None or spec.loader is None:
            return None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _TRAJ_MODULE = module
        _TRAJ_MODULE_MTIME_NS = source_mtime_ns
        return module
    except Exception:
        return None


def _ensure_websocket_lib() -> bool:
    global _HAS_WEBSOCKET, _websocket_lib
    if _HAS_WEBSOCKET and _websocket_lib is not None:
        return True
    try:
        import websocket as _ws  # type: ignore[reportMissingImports]
        _websocket_lib = _ws
        _HAS_WEBSOCKET = True
        return True
    except ImportError:
        _HAS_WEBSOCKET = False
        return False


def _next_ws_client_id() -> int:
    global _ws_client_id_counter
    with _ws_client_id_lock:
        _ws_client_id_counter += 1
        return _ws_client_id_counter


def _write_log_line(line: str) -> None:
    """Thread-safe append to log with rotation."""
    try:
        parsed_line = json.loads(line)
        if not isinstance(parsed_line, dict) or "event" not in parsed_line:
            raise ValueError("Unstructured log entry")
        serialized_line = json.dumps(parsed_line, ensure_ascii=True) + "\n"
    except (json.JSONDecodeError, ValueError, TypeError):
        serialized_line = json.dumps({
            "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "event": "log",
            "robot": None,
            "payload": {"message": line.strip()},
        }, ensure_ascii=True) + "\n"

    with _log_lock:
        try:
            existing = LOG_FILE.read_text(encoding="utf-8").splitlines(keepends=True) if LOG_FILE.exists() else []
            lines = (existing + [serialized_line])[-LOG_MAX_LINES:]
            LOG_FILE.write_text("".join(lines), encoding="utf-8")
        except Exception as exc:
            print(f"[WSAPIClient] Failed to write log.json: {exc}")


# Per-robot WebSocket state
_robot_ws_stop: Dict[str, threading.Event] = {}
_robot_ws_status_buf: Dict[str, Any] = {}   # latest data, sent every 100 ms
_robot_ws_flush_timer: Dict[str, threading.Timer] = {}
_robot_ws_axes_buf: Dict[str, Any] = {}     # latest act_values_axes data
_robot_ws_axes_flush_timer: Dict[str, threading.Timer] = {}
_robot_ws_tcp_buf: Dict[str, Any] = {}      # latest act_values_tcp data
_robot_ws_tcp_flush_timer: Dict[str, threading.Timer] = {}
_robot_ws_last_message_at: Dict[str, float] = {}
_robot_cmd_ws: Dict[str, Any] = {}
_robot_cmd_ws_lock = threading.Lock()
_robot_cmd_io_locks: Dict[str, threading.Lock] = {}


def _log_ws(message: str) -> None:
    _write_log_line(json.dumps({
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "event": "ws_info",
        "robot": None,
        "payload": {"message": message},
    }, ensure_ascii=True))


def _log_robot_command_event(robot: str, event: str, payload: Dict[str, Any]) -> None:
    line = {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "event": event,
        "robot": robot,
        "payload": payload,
    }
    _write_log_line(json.dumps(line, ensure_ascii=True) + "\n")


def _flush_robot_status(name: str) -> None:
    data = _robot_ws_status_buf.get(name)
    if data is not None:
        _request_json("POST", "/api/robot-status-notify", {"robot": name, "data": data})


def _flush_robot_axes(name: str) -> None:
    data = _robot_ws_axes_buf.get(name)
    if data is not None:
        _request_json("POST", "/api/robot-axes-notify", {"robot": name, "data": data})


def _flush_robot_tcp(name: str) -> None:
    data = _robot_ws_tcp_buf.get(name)
    if data is not None:
        _request_json("POST", "/api/robot-tcp-notify", {"robot": name, "data": data})


def _start_robot_ws(name: str) -> None:
    if not _ensure_websocket_lib():
        _log_ws(f"[WS][{name}] websocket-client is not installed - pip install websocket-client")
        return
    # Already running — reconnect is handled internally by _run_robot_ws
    if name in _robot_ws_stop and not _robot_ws_stop[name].is_set():
        return
    stop = threading.Event()
    _robot_ws_stop[name] = stop
    threading.Thread(target=_run_robot_ws, args=(name, stop), daemon=True).start()


def _run_robot_ws(name: str, stop: threading.Event) -> None:
    ip = _read_robot_ip()
    if not ip or not LAST_ACCESS_TOKEN:
        return
    url = f"ws://{ip}{_ROBOT_URL_PATH_WS.format(name=name)}?auth_token={LAST_ACCESS_TOKEN}"
    req_id = [1]
    flush_timer: list = [None]

    def _schedule_flush() -> None:
        if flush_timer[0]:
            flush_timer[0].cancel()
        t = threading.Timer(0.1, _flush_robot_status, args=(name,))
        t.daemon = True
        t.start()
        flush_timer[0] = t

    def on_open(ws):
        sub = json.dumps({"request": req_id[0], "cmd": "subscribe_topic",
                          "args": {"name": "robot_status", "cycle_time_s": 0.1}})
        ws.send(sub)
        _log_ws(f"[WS][{name}] subscribe cmd={sub}")
        sub2 = json.dumps({"request": 2, "cmd": "subscribe_topic",
                           "args": {"name": "act_values_axes", "cycle_time_s": 0.1}})
        ws.send(sub2)
        _log_ws(f"[WS][{name}] subscribe cmd={sub2}")
        sub3 = json.dumps({"request": 3, "cmd": "subscribe_topic",
                           "args": {"name": "act_values_tcp", "cycle_time_s": 0.1}})
        ws.send(sub3)
        _log_ws(f"[WS][{name}] subscribe cmd={sub3}")

    def on_message(ws, message):
        if stop.is_set():
            ws.close()
            return
        _robot_ws_last_message_at[name] = time.monotonic()
        try:
            msg = json.loads(message)
        except Exception as exc:
            _log_ws(f"[WS][{name}] recv (non-JSON): {message[:200]}")
            return
        if msg.get("response") == req_id[0] or msg.get("response") in (2, 3):
            status = msg.get("status", "?")
            err = msg.get("error", {})
            err_str = f" error={err}" if err else ""
            _log_ws(f"[WS][{name}] subscribe response status={status}{err_str}")
            return
        if msg.get("topic") == "robot_status":
            _robot_ws_status_buf[name] = msg["data"]
            _schedule_flush()
            return
        if msg.get("topic") == "act_values_axes":
            _robot_ws_axes_buf[name] = msg["data"]
            if name in _robot_ws_axes_flush_timer and _robot_ws_axes_flush_timer[name]:
                _robot_ws_axes_flush_timer[name].cancel()
            t = threading.Timer(0.1, _flush_robot_axes, args=(name,))
            t.daemon = True
            t.start()
            _robot_ws_axes_flush_timer[name] = t
            return
        if msg.get("topic") == "act_values_tcp":
            _robot_ws_tcp_buf[name] = msg["data"]
            if name in _robot_ws_tcp_flush_timer and _robot_ws_tcp_flush_timer[name]:
                _robot_ws_tcp_flush_timer[name].cancel()
            t = threading.Timer(0.1, _flush_robot_tcp, args=(name,))
            t.daemon = True
            t.start()
            _robot_ws_tcp_flush_timer[name] = t
            return
        # log any unexpected message for diagnosis
        _log_ws(f"[WS][{name}] unexpected msg={message[:200]}")

    def on_error(ws, err):
        _log_ws(f"[WS][{name}] error {type(err).__name__}: {err}")

    def on_close(ws, code, reason):
        _log_ws(f"[WS][{name}] closed code={code} reason={reason!r}")
        if flush_timer[0]:
            flush_timer[0].cancel()
        if not stop.is_set() and LAST_REFRESH_TOKEN:
            time.sleep(2)
            if not stop.is_set():
                _run_robot_ws(name, stop)

    ws = _websocket_lib.WebSocketApp(url, on_open=on_open, on_message=on_message,
                                      on_error=on_error, on_close=on_close)
    _log_ws(f"[WS][{name}] connecting url={url[:url.index('auth_token=')+20]}...")
    ws.run_forever()


def _stop_all_robot_ws() -> None:
    for ev in list(_robot_ws_stop.values()):
        ev.set()
    _robot_ws_stop.clear()
    _robot_ws_status_buf.clear()
    _robot_ws_axes_buf.clear()
    _robot_ws_tcp_buf.clear()
    _robot_ws_last_message_at.clear()


def _close_robot_cmd_ws(name: str) -> None:
    ws = None
    with _robot_cmd_ws_lock:
        ws = _robot_cmd_ws.pop(name, None)
    if ws is not None:
        try:
            ws.close()
        except Exception:
            pass


def _close_all_robot_cmd_ws() -> None:
    with _robot_cmd_ws_lock:
        names = list(_robot_cmd_ws.keys())
    for name in names:
        _close_robot_cmd_ws(name)


def _get_robot_cmd_ws(robot: str, ws_url: str):
    with _robot_cmd_ws_lock:
        existing = _robot_cmd_ws.get(robot)
    if existing is not None:
        return existing, False

    ws = _websocket_lib.create_connection(ws_url, timeout=5)
    client_resp = _ws_send_cmd_and_wait(ws, 1000, "set_active_client", timeout_s=2.5)
    if int(client_resp.get("status", 0) or 0) != 200:
        try:
            ws.close()
        except Exception:
            pass
        raise RuntimeError(f"set_active_client failed: {client_resp}")

    with _robot_cmd_ws_lock:
        _robot_cmd_ws[robot] = ws
    return ws, True


def _get_robot_cmd_io_lock(robot: str) -> threading.Lock:
    with _robot_cmd_ws_lock:
        lock = _robot_cmd_io_locks.get(robot)
        if lock is None:
            lock = threading.Lock()
            _robot_cmd_io_locks[robot] = lock
        return lock


def _read_robot_ip() -> str:
    try:
        for line in PARAMETERS_PRM.read_text(encoding="utf-8").splitlines():
            if line.startswith("api_ip="):
                return line[len("api_ip="):].strip()
    except Exception:
        pass
    return ""


def _robot_login_url() -> str:
    ip = _read_robot_ip()
    return f"http://{ip}{_ROBOT_URL_PATH_LOGIN}" if ip else ""


def _robot_logout_url() -> str:
    ip = _read_robot_ip()
    return f"http://{ip}{_ROBOT_URL_PATH_LOGOUT}" if ip else ""


def _robot_refresh_url() -> str:
    ip = _read_robot_ip()
    return f"http://{ip}{_ROBOT_URL_PATH_REFRESH}" if ip else ""


def _robot_list_url() -> str:
    ip = _read_robot_ip()
    return f"http://{ip}{_ROBOT_URL_PATH_ROBOTS}" if ip else ""


def _fetch_and_notify_robot_list() -> None:
    robot_url = _robot_list_url()
    if not robot_url or not LAST_ACCESS_TOKEN:
        return
    headers = {"Accept": "application/json", "Authorization": f"Bearer {LAST_ACCESS_TOKEN}"}
    req = request.Request(url=robot_url, headers=headers, method="GET")
    try:
        with request.urlopen(req, timeout=3) as response:
            raw = response.read().decode("utf-8")
            _write_log_line(f"[{__import__('time').strftime('%Y-%m-%d %H:%M:%S')}] [robot-list] raw={raw[:500]}\n")
            robots = json.loads(raw) if raw else []
            _request_json("POST", "/api/robot-list-notify", {"robots": robots})
            for name in robots:
                _start_robot_ws(name)
    except Exception as exc:
        _write_log_line(f"[{__import__('time').strftime('%Y-%m-%d %H:%M:%S')}] [robot-list] error {exc}\n")


def _fetch_and_notify_robot_list_async() -> None:
    t = threading.Thread(target=_fetch_and_notify_robot_list, daemon=True)
    t.start()


def _execute_robot_refresh() -> None:
    global LAST_ACCESS_TOKEN, LAST_REFRESH_TOKEN, _last_refresh_ok
    robot_url = _robot_refresh_url()
    if not robot_url or not LAST_REFRESH_TOKEN:
        return
    body = json.dumps({"refresh_token": LAST_REFRESH_TOKEN}).encode("utf-8")
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LAST_REFRESH_TOKEN}",
    }
    req = request.Request(url=robot_url, data=body, headers=headers, method="POST")
    try:
        with request.urlopen(req, timeout=1) as response:
            raw = response.read().decode("utf-8")
            parsed: Any = {}
            if raw:
                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError:
                    parsed = {"raw": raw}
            _trace_response(robot_url, response.status, parsed)
            LAST_ACCESS_TOKEN = str(parsed.get("access_token", LAST_ACCESS_TOKEN)).strip() or LAST_ACCESS_TOKEN
            LAST_REFRESH_TOKEN = str(parsed.get("refresh_token", LAST_REFRESH_TOKEN)).strip() or LAST_REFRESH_TOKEN
            if 200 <= response.status < 300:
                _last_refresh_ok = True
                _request_json("POST", "/api/refresh-notify", {"ok": True, "access_token": LAST_ACCESS_TOKEN})
                _save_session()
    except Exception:
        _last_refresh_ok = False
        _request_json("POST", "/api/refresh-notify", {"ok": False})


def _stop_refresh_timer() -> None:
    global _refresh_timer
    if _refresh_timer:
        _refresh_timer.cancel()
        _refresh_timer = None


def _schedule_refresh() -> None:
    global _refresh_timer
    _refresh_timer = threading.Timer(_REFRESH_INTERVAL, _refresh_cycle)
    _refresh_timer.daemon = True
    _refresh_timer.start()


def _refresh_cycle() -> None:
    has_active_ws = any(
        time.monotonic() - last_message_at <= 2 * _REFRESH_INTERVAL
        for last_message_at in _robot_ws_last_message_at.values()
    )
    if has_active_ws:
        _schedule_refresh()
        return

    prev_ok = _last_refresh_ok
    _execute_robot_refresh()
    if LAST_REFRESH_TOKEN:
        # Re-fetch robot list only when recovering from a previous failure.
        if not prev_ok and _last_refresh_ok:
            _fetch_and_notify_robot_list_async()
        _schedule_refresh()


# Suppress repeated connection-refused noise: log only on first failure and every 10th after.
_CONSECUTIVE_CONN_ERRORS = 0


def _save_session() -> None:
    try:
        SESSION_FILE.write_text(
            f"access_token={LAST_ACCESS_TOKEN}\nrefresh_token={LAST_REFRESH_TOKEN}\n",
            encoding="utf-8",
        )
    except Exception:
        pass


def _clear_session() -> None:
    try:
        SESSION_FILE.write_text("access_token=\nrefresh_token=\n", encoding="utf-8")
    except Exception:
        pass


def _restore_session() -> None:
    global LAST_ACCESS_TOKEN, LAST_REFRESH_TOKEN
    if not SESSION_FILE.exists():
        return
    data = {}
    for line in SESSION_FILE.read_text(encoding="utf-8").splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            data[k.strip()] = v.strip()
    access = data.get("access_token", "")
    refresh = data.get("refresh_token", "")
    if not refresh:
        return
    LAST_ACCESS_TOKEN = access
    LAST_REFRESH_TOKEN = refresh
    # Test session and resume background flows when refresh succeeds.
    _execute_robot_refresh()
    if LAST_REFRESH_TOKEN and _last_refresh_ok:
        _request_json("POST", "/api/refresh-notify", {"ok": True, "access_token": LAST_ACCESS_TOKEN})
        _fetch_and_notify_robot_list_async()
        _stop_refresh_timer()
        _schedule_refresh()


def _trace_post(direction: str, target: str, payload: Optional[Dict[str, Any]]) -> None:
    pass


def _trace_response(source: str, status_code: int, response_body: Any) -> None:
    successful_notification_paths = {
        "/api/robot-list-notify",
        "/api/robot-status-notify",
        "/api/robot-axes-notify",
        "/api/robot-tcp-notify",
    }
    if source in successful_notification_paths and status_code == 200:
        return

    _write_log_line(json.dumps({
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "event": "http_response",
        "robot": None,
        "payload": {
            "source": source,
            "status": status_code,
            "response": response_body,
        },
    }, ensure_ascii=True))

# Restrict command execution to a known-safe set of prefixes.
ALLOWED_PREFIXES = (
    "echo",
    "dir",
    "whoami",
    "hostname",
    "ipconfig",
    "ping",
)


def _request_json(method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    url = f"{SERVER_BASE_URL}{path}"
    data = None
    headers = {"Accept": "application/json"}

    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    if method.upper() == "POST":
        _trace_post("OUT", url, payload)

    req = request.Request(url=url, data=data, headers=headers, method=method)

    # Paths too frequent to log (robot status at 10 Hz)
    _no_log_paths = {"/api/robot-status-notify", "/api/refresh-notify"}

    try:
        with request.urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            raw = response.read().decode("utf-8")
            parsed: Any = {}
            if not raw:
                if method.upper() == "POST" and path not in _no_log_paths:
                    _trace_response(path, response.status, parsed)
                return {}
            parsed = json.loads(raw)
            if method.upper() == "POST" and path not in _no_log_paths:
                _trace_response(path, response.status, parsed)
            return parsed
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        parsed_body: Any = body
        try:
            parsed_body = json.loads(body)
        except json.JSONDecodeError:
            parsed_body = body
        if method.upper() == "POST":
            _trace_response(path, exc.code, parsed_body)
        print(f"[WSAPIClient] HTTP {exc.code} on {method} {path}")
        return None
    except error.URLError as exc:
        global _CONSECUTIVE_CONN_ERRORS
        _CONSECUTIVE_CONN_ERRORS += 1
        if _CONSECUTIVE_CONN_ERRORS == 1 or _CONSECUTIVE_CONN_ERRORS % 10 == 0:
            print(
                f"[WSAPIClient] Server unreachable for {method} {path}: {exc.reason} "
                f"(attempt #{_CONSECUTIVE_CONN_ERRORS} - is WebServeur4WSAPI running?)"
            )
        return None
    except json.JSONDecodeError:
        print(f"[WSAPIClient] Invalid JSON response on {method} {path}")
        return None


def _request_json_absolute(method: str, url: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    data = None
    headers = {"Accept": "application/json"}

    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    if method.upper() == "POST":
        _trace_post("OUT", url, payload)

    req = request.Request(url=url, data=data, headers=headers, method=method)

    try:
        with request.urlopen(req, timeout=ROBOT_LOGIN_TIMEOUT_SECONDS) as response:
            raw = response.read().decode("utf-8")
            parsed = None
            if raw:
                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError:
                    parsed = {"raw": raw}
            if method.upper() == "POST":
                _trace_response(url, response.status, parsed if parsed is not None else {})

            return {
                "ok": 200 <= response.status < 300,
                "status_code": response.status,
                "response": parsed,
            }
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        parsed_body: Any = body
        try:
            parsed_body = json.loads(body)
        except json.JSONDecodeError:
            parsed_body = body
        if method.upper() == "POST":
            _trace_response(url, exc.code, parsed_body)
        return {
            "ok": False,
            "status_code": exc.code,
            "response": parsed_body,
        }
    except error.URLError as exc:
        return {
            "ok": False,
            "status_code": 0,
            "response": f"Connection error: {exc.reason}",
        }


def _execute_robot_login(command_payload: Dict[str, Any]) -> Dict[str, Any]:
    robot_url = _robot_login_url()
    username = str(command_payload.get("username", "")).strip()
    password = str(command_payload.get("password", "")).strip()

    if not robot_url:
        return {"ok": False, "status_code": 0, "response": "Missing robot login URL (api_ip not set in WebServeurParameters.prm)"}

    if not username or not password:
        return {"ok": False, "status_code": 0, "response": "Username or password missing in WebServeurParameters.prm"}

    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    _write_log_line(f"[{timestamp}] [LOGIN] url={robot_url} username={username!r} password_len={len(password)}\n")

    payload = {
        "username": username,
        "password": password,
    }
    result = _request_json_absolute("POST", robot_url, payload)

    global LAST_ACCESS_TOKEN, LAST_REFRESH_TOKEN
    response_obj = result.get("response")
    if result.get("ok") and isinstance(response_obj, dict):
        token = str(response_obj.get("access_token", "")).strip()
        if token:
            LAST_ACCESS_TOKEN = token
        rtoken = str(response_obj.get("refresh_token", "")).strip()
        if rtoken:
            LAST_REFRESH_TOKEN = rtoken
            _stop_refresh_timer()
            _save_session()
            _request_json("POST", "/api/refresh-notify", {"ok": True, "access_token": LAST_ACCESS_TOKEN})
            _fetch_and_notify_robot_list_async()
            _schedule_refresh()

    return result


def _execute_robot_logout(_command_payload: Dict[str, Any]) -> Dict[str, Any]:
    _stop_refresh_timer()
    _stop_all_robot_ws()
    _close_all_robot_cmd_ws()
    global LAST_REFRESH_TOKEN
    LAST_REFRESH_TOKEN = ""
    _clear_session()
    _request_json("POST", "/api/robot-list-notify", {"robots": []})
    robot_url = _robot_logout_url()
    if not robot_url:
        return {"ok": False, "status_code": 0, "response": "Missing robot logout URL (api_ip not set in WebServeurParameters.prm)"}
    data = b""
    headers = {"Accept": "*/*"}

    if LAST_ACCESS_TOKEN:
        headers["Authorization"] = f"Bearer {LAST_ACCESS_TOKEN}"

    _trace_post("OUT", robot_url, {})
    req = request.Request(url=robot_url, data=data, headers=headers, method="POST")

    try:
        with request.urlopen(req, timeout=ROBOT_LOGIN_TIMEOUT_SECONDS) as response:
            raw = response.read().decode("utf-8")
            parsed: Any = {}
            if raw:
                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError:
                    parsed = {"raw": raw}
            _trace_response(robot_url, response.status, parsed)
            return {
                "ok": 200 <= response.status < 300,
                "status_code": response.status,
                "response": parsed,
            }
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        parsed_body: Any = body
        try:
            parsed_body = json.loads(body)
        except json.JSONDecodeError:
            parsed_body = body
        _trace_response(robot_url, exc.code, parsed_body)
        return {
            "ok": False,
            "status_code": exc.code,
            "response": parsed_body,
        }
    except error.URLError as exc:
        return {
            "ok": False,
            "status_code": 0,
            "response": f"Connection error: {exc.reason}",
        }


def _execute_robot_traj(command_payload: Dict[str, Any]) -> Dict[str, Any]:
    robot = str(command_payload.get("robot", "")).strip()
    trajectory = str(command_payload.get("trajectory", "")).strip()
    current_tcp = command_payload.get("current_tcp", {})
    workspace_dir = Path(__file__).resolve().parent

    if not robot:
        return {"ok": False, "status_code": 0, "response": "Missing robot name"}
    if not trajectory:
        return {"ok": False, "status_code": 0, "response": "Missing trajectory file"}
    if not TRAJ_SCRIPT.exists():
        return {"ok": False, "status_code": 0, "response": "WSAPITraj.py not found"}

    trajectory_path = workspace_dir / "InitTrajFiles" / trajectory
    if not trajectory_path.exists():
        return {
            "ok": False,
            "status_code": 0,
            "response": f"Trajectory file not found: {trajectory}",
        }

    cmd = [
        sys.executable,
        str(TRAJ_SCRIPT),
        "--robot",
        robot,
        "--trajectory",
        str(trajectory_path),
    ]

    robot_ip = _read_robot_ip()
    if robot_ip:
        cmd.extend(["--api-ip", robot_ip])
    if LAST_ACCESS_TOKEN:
        cmd.extend(["--access-token", LAST_ACCESS_TOKEN])
    if isinstance(current_tcp, dict) and current_tcp:
        cmd.extend(["--tcp-pos", json.dumps(current_tcp, ensure_ascii=True)])

    try:
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=90,
            cwd=str(workspace_dir),
        )
    except subprocess.TimeoutExpired:
        return {
            "ok": False,
            "status_code": 0,
            "response": "WSAPITraj timeout after 90s",
        }
    except Exception as exc:
        return {
            "ok": False,
            "status_code": 0,
            "response": f"WSAPITraj execution error: {exc}",
        }

    stdout = (completed.stdout or "").strip()
    stderr = (completed.stderr or "").strip()

    parsed: Dict[str, Any] = {}
    if stdout:
        last_line = stdout.splitlines()[-1]
        try:
            maybe = json.loads(last_line)
            if isinstance(maybe, dict):
                parsed = maybe
        except json.JSONDecodeError:
            parsed = {}

    if completed.returncode != 0:
        return {
            "ok": False,
            "status_code": 0,
            "response": parsed if parsed else {"stdout": stdout, "stderr": stderr},
        }

    if parsed:
        return parsed

    return {
        "ok": True,
        "status_code": 200,
        "response": {"stdout": stdout, "stderr": stderr},
    }


def _robot_temptraj_file(robot: str) -> Path:
    lower = robot.lower()
    suffix = "r2" if ("2" in lower or "r2" in lower) else "r1"
    return Path(__file__).resolve().parent / "TempTraj" / f"temptraj_{suffix}.csv"


def _execute_robot_traj_start(command_payload: Dict[str, Any]) -> Dict[str, Any]:
    robot = str(command_payload.get("robot", "")).strip()
    overlap_percent = command_payload.get("overlap_percent", 100)
    workspace_dir = Path(__file__).resolve().parent

    if not robot:
        return {"ok": False, "status_code": 0, "response": "Missing robot name"}
    if not TRAJ_SCRIPT.exists():
        return {"ok": False, "status_code": 0, "response": "WSAPITraj.py not found"}

    # Trajectory start uses its own websocket-command client. Do not close the
    # persistent command socket preemptively because some controllers couple
    # power state to active-client presence.

    temptraj = _robot_temptraj_file(robot)
    if not temptraj.exists():
        return {
            "ok": False,
            "status_code": 0,
            "response": f"Temp trajectory not found: {temptraj.name}. Run Selection first.",
        }

    cmd = [
        sys.executable,
        str(TRAJ_SCRIPT),
        "--start",
        "--queue-only",
        "--robot",
        robot,
        "--trajectory",
        str(temptraj),
        "--overlap-percent",
        str(overlap_percent),
    ]

    robot_ip = _read_robot_ip()
    if robot_ip:
        cmd.extend(["--api-ip", robot_ip])
    if LAST_ACCESS_TOKEN:
        cmd.extend(["--access-token", LAST_ACCESS_TOKEN])

    # Preferred path: run trajectory on the same persistent websocket-command
    # client used for regular robot commands.
    traj_module = _get_traj_module()
    if traj_module is not None and robot_ip and LAST_ACCESS_TOKEN:
        starter = getattr(traj_module, "start_trajectory_with_shared_ws", None)
        if callable(starter):
            last_exc: Optional[Exception] = None
            for attempt in range(1, 4):
                try:
                    ws_url = _robot_ws_command_url(robot)
                    if not ws_url:
                        raise RuntimeError("Missing robot IP (api_ip)")
                    io_lock = _get_robot_cmd_io_lock(robot)
                    with io_lock:
                        ws, _ = _get_robot_cmd_ws(robot, ws_url)
                        info = starter(
                            robot=robot,
                            csv_path=temptraj,
                            ws=ws,
                            overlap_percent=float(overlap_percent),
                            api_ip=robot_ip,
                            access_token=LAST_ACCESS_TOKEN,
                            start_execution=False,
                        )
                    return {
                        "ok": True,
                        "status_code": 200,
                        "response": {
                            "message": "Trajectory points queued",
                            "robot": robot,
                            "trajectory": temptraj.name,
                            **(info if isinstance(info, dict) else {}),
                        },
                    }
                except Exception as exc:
                    last_exc = exc
                    msg = str(exc)
                    if ("Connection to remote host was lost" in msg or "too_many_connections" in msg) and attempt < 3:
                        _close_robot_cmd_ws(robot)
                        time.sleep(0.25 * attempt)
                        continue
                    break
            if last_exc is not None:
                return {
                    "ok": False,
                    "status_code": 0,
                    "response": {
                        "ok": False,
                        "status_code": 500,
                        "response": {
                            "message": f"Unable to start trajectory: {last_exc}",
                            "trajectory": str(temptraj),
                        },
                    },
                }

    completed = None
    parsed: Dict[str, Any] = {}
    stdout = ""
    stderr = ""
    for attempt in range(1, 4):
        try:
            completed = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,
                cwd=str(workspace_dir),
            )
        except subprocess.TimeoutExpired:
            return {
                "ok": False,
                "status_code": 0,
                "response": "WSAPITraj start timeout after 300s",
            }
        except Exception as exc:
            return {
                "ok": False,
                "status_code": 0,
                "response": f"WSAPITraj start execution error: {exc}",
            }

        stdout = (completed.stdout or "").strip()
        stderr = (completed.stderr or "").strip()
        parsed = {}
        if stdout:
            try:
                maybe = json.loads(stdout.splitlines()[-1])
                if isinstance(maybe, dict):
                    parsed = maybe
            except json.JSONDecodeError:
                parsed = {}

        if completed.returncode == 0:
            if parsed:
                return parsed
            return {
                "ok": True,
                "status_code": 200,
                "response": {
                    "message": "Trajectory start completed",
                    "robot": robot,
                    "trajectory": temptraj.name,
                    "stdout": stdout,
                    "stderr": stderr,
                },
            }

        combined_error = (stdout + "\n" + stderr).lower()
        retryable = "connection to remote host was lost" in combined_error or "too_many_connections" in combined_error
        if retryable and attempt < 3:
            _close_robot_cmd_ws(robot)
            time.sleep(0.25 * attempt)
            continue
        break

    return {
        "ok": False,
        "status_code": 0,
        "response": parsed if parsed else {"stdout": stdout, "stderr": stderr},
    }


def _robot_ws_command_url(robot: str) -> str:
    ip = _read_robot_ip()
    if not ip:
        return ""
    return f"ws://{ip}{_ROBOT_URL_PATH_WS_CMD.format(name=robot)}?auth_token={LAST_ACCESS_TOKEN}"


def _ws_send_cmd_and_wait(
    ws,
    request_id: int,
    cmd: str,
    args: Optional[Dict[str, Any]] = None,
    timeout_s: float = 8.0,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"request": request_id, "cmd": cmd}
    if isinstance(args, dict):
        payload["args"] = args
    ws.send(json.dumps(payload, ensure_ascii=True))

    deadline = time.time() + timeout_s
    while time.time() < deadline:
        raw = ws.recv()
        msg = json.loads(raw)
        if msg.get("response") == request_id:
            return msg
    return {"status": 0, "error": {"key": "timeout", "msg": f"No response for cmd {cmd}"}}


def _execute_robot_cmd(command_payload: Dict[str, Any]) -> Dict[str, Any]:
    if not _ensure_websocket_lib():
        return {"ok": False, "status_code": 0, "response": "websocket-client not installed"}

    robot = str(command_payload.get("robot", "")).strip()
    action = str(command_payload.get("action", "")).strip().lower()

    if not robot:
        return {"ok": False, "status_code": 0, "response": "Missing robot name"}
    if not LAST_ACCESS_TOKEN:
        return {"ok": False, "status_code": 0, "response": "Missing access token. Login first."}

    ws_url = _robot_ws_command_url(robot)
    if not ws_url:
        return {"ok": False, "status_code": 0, "response": "Missing robot IP (api_ip)"}

    def _current_robot_state() -> str:
        snapshot = _request_json("GET", "/api/robot-status") or {}
        if not isinstance(snapshot, dict):
            return ""
        robot_data = snapshot.get(robot, {})
        if not isinstance(robot_data, dict):
            return ""
        state = robot_data.get("robot_state")
        return str(state) if state is not None else ""

    def _current_robot_has_error() -> Optional[bool]:
        snapshot = _request_json("GET", "/api/robot-status") or {}
        if not isinstance(snapshot, dict):
            return None
        robot_data = snapshot.get(robot, {})
        if not isinstance(robot_data, dict):
            return None
        if "has_error" not in robot_data:
            return None
        return bool(robot_data.get("has_error", False))

    action_variants: Dict[str, list[tuple[str, Optional[Dict[str, Any]]]]] = {
        "power": [
            ("power", {"state": "on"}),
            ("power_on", None),
        ],
        "power_enable": [
            ("power", {"state": "on"}),
            ("enable_power", {"cmd_name": "ui_power_enable"}),
            ("enable_power", None),
            ("reset_errors", {"cmd_name": "ui_power_enable"}),
            ("power_on", None),
        ],
        "power_disable": [
            ("power", {"state": "off"}),
            ("reset_errors", {"cmd_name": "ui_power_disable"}),
            ("error_reset", None),
            ("reset_error", None),
        ],
        "error_reset": [
            ("reset_errors", {"cmd_name": "ui_error_reset"}),
            ("error_reset", None),
            ("reset_error", None),
        ],
        "path_execution_start": [
            ("set_active_client", None),
            ("clear_path", None),
            ("start_path_execution", None),
        ],
        "path_execution_stop": [
            ("stop_robot", {"mode": "on_path"}),
        ],
    }

    variants = action_variants.get(action)
    if not variants:
        return {"ok": False, "status_code": 0, "response": f"Unsupported action: {action}"}

    cmd_wait_s = 2.5 if action in ("power", "power_enable", "power_disable") else 8.0

    ws = None
    created_ws = False
    io_lock = _get_robot_cmd_io_lock(robot)
    try:
        io_lock.acquire()
        ws, created_ws = _get_robot_cmd_ws(robot, ws_url)
        req_id = 1000

        attempts: list[Dict[str, Any]] = []
        start_state = _current_robot_state()
        for variant_index, (cmd_name, cmd_args) in enumerate(variants):
            last_exc: Optional[Exception] = None
            resp: Dict[str, Any] = {}
            for send_attempt in range(2):
                req_id += 1
                _log_robot_command_event(robot, "cmd_send", {
                    "request": req_id,
                    "cmd": cmd_name,
                    "args": cmd_args,
                    "wait_response": True,
                    "action": action,
                })
                sent_at = time.perf_counter()
                try:
                    resp = _ws_send_cmd_and_wait(ws, req_id, cmd_name, cmd_args, timeout_s=cmd_wait_s)
                except Exception as exc:
                    last_exc = exc
                    _log_robot_command_event(robot, "ws_error", {
                        "request": req_id,
                        "cmd": cmd_name,
                        "action": action,
                        "error": str(exc),
                    })
                    # If the cached ws is stale, reconnect once and retry this variant.
                    _close_robot_cmd_ws(robot)
                    ws, created_ws = _get_robot_cmd_ws(robot, ws_url)
                    continue

                status = int(resp.get("status", 0) or 0)
                response_time_ms = round((time.perf_counter() - sent_at) * 1000.0, 3)
                _log_robot_command_event(robot, f"cmd_response: {response_time_ms}", {
                    "request": req_id,
                    "cmd": cmd_name,
                    "action": action,
                    "status": status,
                    "response": resp,
                })
                err = resp.get("error", {}) if isinstance(resp.get("error"), dict) else {}
                err_key = str(err.get("key", "")).lower()
                if status == 400 and err_key == "not_active_client" and send_attempt == 0:
                    # Another session became active: reclaim active client and replay command once.
                    req_id += 1
                    _ws_send_cmd_and_wait(ws, req_id, "set_active_client", timeout_s=2.5)
                    continue
                break

            if not resp and last_exc is not None:
                raise last_exc
            status = int(resp.get("status", 0) or 0)
            attempts.append({"cmd": cmd_name, "args": cmd_args, "response": resp})
            if status == 200:
                if action in ("path_execution_start", "path_execution_stop") and variant_index < len(variants) - 1:
                    continue
                # Some controllers ACK commands that do not change power state.
                # For power actions, validate robot_state transition before reporting success.
                if action in ("power_enable", "power", "power_disable"):
                    if action == "power_disable":
                        # On this controller, power_disable is applied when the command
                        # websocket client is released, so close it before polling state.
                        _close_robot_cmd_ws(robot)
                    deadline = time.monotonic() + 1.2
                    state = _current_robot_state()
                    while time.monotonic() < deadline:
                        if action == "power_disable" and state == "power_disabled":
                            return {
                                "ok": True,
                                "status_code": 200,
                                "response": {
                                    "action": action,
                                    "cmd": cmd_name,
                                    "raw": resp,
                                    "robot_state": state,
                                    "start_state": start_state,
                                },
                            }
                        if action in ("power_enable", "power") and state and state != "power_disabled":
                            return {
                                "ok": True,
                                "status_code": 200,
                                "response": {
                                    "action": action,
                                    "cmd": cmd_name,
                                    "raw": resp,
                                    "robot_state": state,
                                    "start_state": start_state,
                                },
                            }
                        state = _current_robot_state()
                    if action == "power_disable":
                        # Give a little extra time after ws close before failing over.
                        deadline2 = time.monotonic() + 2.0
                        while time.monotonic() < deadline2:
                            state = _current_robot_state()
                            if state == "power_disabled":
                                return {
                                    "ok": True,
                                    "status_code": 200,
                                    "response": {
                                        "action": action,
                                        "cmd": cmd_name,
                                        "raw": resp,
                                        "robot_state": state,
                                        "start_state": start_state,
                                    },
                                }
                    continue
                if action == "error_reset":
                    # Validate that error state is actually cleared, not only ACKed.
                    has_error = _current_robot_has_error()
                    if has_error is None:
                        return {
                            "ok": True,
                            "status_code": 200,
                            "response": {"action": action, "cmd": cmd_name, "raw": resp},
                        }
                    if not has_error:
                        return {
                            "ok": True,
                            "status_code": 200,
                            "response": {"action": action, "cmd": cmd_name, "raw": resp},
                        }

                    deadline = time.monotonic() + 2.0
                    while time.monotonic() < deadline:
                        has_error = _current_robot_has_error()
                        if has_error is False:
                            return {
                                "ok": True,
                                "status_code": 200,
                                "response": {"action": action, "cmd": cmd_name, "raw": resp},
                            }
                    # ACK received but error still active; try next variant.
                    continue
                return {
                    "ok": True,
                    "status_code": 200,
                    "response": {"action": action, "cmd": cmd_name, "raw": resp},
                }

        last_status = int(attempts[-1]["response"].get("status", 0) or 0) if attempts else 0
        message = f"No command variant accepted for action {action}"
        if action == "error_reset":
            message = "Error reset command acknowledged but robot has_error remains true"
        return {
            "ok": False,
            "status_code": last_status,
            "response": {
                "message": message,
                "attempts": attempts,
            },
        }
    except Exception as exc:
        _close_robot_cmd_ws(robot)
        return {
            "ok": False,
            "status_code": 0,
            "response": f"robot_cmd execution error: {exc}",
        }
    finally:
        # Keep command websocket open to retain active client after power enable.
        # If we just created the socket but command failed, clean it up.
        if created_ws:
            with _robot_cmd_ws_lock:
                still_cached = _robot_cmd_ws.get(robot)
            if ws is not still_cached:
                try:
                    if ws is not None:
                        ws.close()
                except Exception:
                    pass
        if io_lock.locked():
            io_lock.release()


def _is_allowed(command: str) -> bool:
    normalized = command.strip().lower()
    return normalized.startswith(ALLOWED_PREFIXES)


def _execute_command(command: str) -> Dict[str, Any]:
    if not _is_allowed(command):
        return {
            "ok": False,
            "exit_code": -1,
            "stdout": "",
            "stderr": "Command blocked by client policy",
        }

    try:
        completed = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=COMMAND_TIMEOUT_SECONDS,
        )
        return {
            "ok": completed.returncode == 0,
            "exit_code": completed.returncode,
            "stdout": completed.stdout,
            "stderr": completed.stderr,
        }
    except subprocess.TimeoutExpired:
        return {
            "ok": False,
            "exit_code": -2,
            "stdout": "",
            "stderr": f"Command timed out after {COMMAND_TIMEOUT_SECONDS}s",
        }
    except Exception as exc:  # Defensive guard for runtime execution errors.
        return {
            "ok": False,
            "exit_code": -3,
            "stdout": "",
            "stderr": f"Execution error: {exc}",
        }


def _process_one_cycle() -> None:
    global _CONSECUTIVE_CONN_ERRORS, _session_restored
    command_payload = _request_json("GET", "/api/command")
    if not command_payload:
        return
    # Connection restored - reset the error counter.
    if _CONSECUTIVE_CONN_ERRORS > 0:
        print(f"[WSAPIClient] Connection to server restored after {_CONSECUTIVE_CONN_ERRORS} error(s).")
        _CONSECUTIVE_CONN_ERRORS = 0

    if not _session_restored:
        _session_restored = True
        threading.Thread(target=_restore_session, daemon=True).start()

    if command_payload.get("status") == "no_command":
        return

    command_id = command_payload.get("id", "unknown")
    command_type = command_payload.get("type", "shell")

    if command_type == "robot_login":
        print(
            f"[WSAPIClient] Received robot login request #{command_id} "
            f"target={command_payload.get('url', _robot_login_url())}"
        )
        result = _execute_robot_login(command_payload)
        post_payload = {
            "id": command_id,
            "type": command_type,
            "result": result,
        }
        response = _request_json("POST", "/api/command-result", post_payload)

        if response is None:
            print(f"[WSAPIClient] Failed to post result for command #{command_id}")
        else:
            print(f"[WSAPIClient] Result sent for command #{command_id}")
        return

    if command_type == "robot_logout":
        print(
            f"[WSAPIClient] Received robot logout request #{command_id} "
            f"target={command_payload.get('url', _robot_logout_url())}"
        )
        result = _execute_robot_logout(command_payload)
        post_payload = {
            "id": command_id,
            "type": command_type,
            "result": result,
        }
        response = _request_json("POST", "/api/command-result", post_payload)

        if response is None:
            print(f"[WSAPIClient] Failed to post result for command #{command_id}")
        else:
            print(f"[WSAPIClient] Result sent for command #{command_id}")
        return

    if command_type == "robot_traj":
        print(
            f"[WSAPIClient] Received trajectory request #{command_id} "
            f"robot={command_payload.get('robot')} file={command_payload.get('trajectory')}"
        )
        result = _execute_robot_traj(command_payload)
        post_payload = {
            "id": command_id,
            "type": command_type,
            "result": result,
        }
        response = _request_json("POST", "/api/command-result", post_payload)

        if response is None:
            print(f"[WSAPIClient] Failed to post result for command #{command_id}")
        else:
            print(f"[WSAPIClient] Result sent for command #{command_id}")
        return

    if command_type == "robot_traj_start":
        print(
            f"[WSAPIClient] Received trajectory start request #{command_id} "
            f"robot={command_payload.get('robot')}"
        )
        result = _execute_robot_traj_start(command_payload)
        post_payload = {
            "id": command_id,
            "type": command_type,
            "result": result,
        }
        response = _request_json("POST", "/api/command-result", post_payload)

        if response is None:
            print(f"[WSAPIClient] Failed to post result for command #{command_id}")
        else:
            print(f"[WSAPIClient] Result sent for command #{command_id}")
        return

    if command_type == "robot_cmd":
        print(
            f"[WSAPIClient] Received robot cmd request #{command_id} "
            f"robot={command_payload.get('robot')} action={command_payload.get('action')}"
        )
        result = _execute_robot_cmd(command_payload)
        post_payload = {
            "id": command_id,
            "type": command_type,
            "result": result,
        }
        response = _request_json("POST", "/api/command-result", post_payload)

        if response is None:
            print(f"[WSAPIClient] Failed to post result for command #{command_id}")
        else:
            print(f"[WSAPIClient] Result sent for command #{command_id}")
        return

    command_text = command_payload.get("command")

    if not command_text:
        return

    print(f"[WSAPIClient] Received command #{command_id}: {command_text}")
    result = _execute_command(command_text)

    post_payload = {
        "id": command_id,
        "command": command_text,
        "result": result,
    }
    response = _request_json("POST", "/api/command-result", post_payload)

    if response is None:
        print(f"[WSAPIClient] Failed to post result for command #{command_id}")
    else:
        print(f"[WSAPIClient] Result sent for command #{command_id}")


def run_client() -> None:
    print(f"WSAPIClient started. Server: {SERVER_BASE_URL}")
    print("Waiting for commands via long-poll on /api/command")

    while True:
        _process_one_cycle()


if __name__ == "__main__":
    try:
        run_client()
    except KeyboardInterrupt:
        print("\nWSAPIClient stopped.")
