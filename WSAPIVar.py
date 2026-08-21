#!/usr/bin/env python3
"""WSAPIVar
Reads controller variables over the variable-service WebSocket
(asyncapi: KEBA Variables WebSocket, /api/v4/variable-service/variables)
at a 4 ms cycle and forwards the latest values to the local Web API server.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any, Callable, Optional
from urllib import request as http_request

try:
    import websocket
except ImportError:
    websocket = None


VARIABLE_SERVICE_PATH = "/api/v4/variable-service/variables"
TOPIC_NAME = "webapi_vars"
# Server-enforced minimum (asyncapi subscribe_topic_request.args.cycle_time_ms: minimum 10).
MIN_CYCLE_TIME_MS = 10
CYCLE_TIME_MS = 4
DEFAULT_VARIABLES = [
    "APPL.Application.RobotPreUpdate.TrackingFrame.x",
    "APPL.Application.RobotPreUpdate.TrackingFrame.y",
    "APPL.Application.RobotPreUpdate.TrackingFrame.z",
    "APPL.Application.RobotPreUpdate.TrackingFrame.a",
    "APPL.Application.RobotPreUpdate.TrackingFrame.b",
    "APPL.Application.RobotPreUpdate.TrackingFrame.c",
]


def _post_json(local_url: str, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
    req = http_request.Request(local_url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with http_request.urlopen(req, timeout=3):
            pass
    except Exception:
        pass


def _send_command(ws: Any, request_id: int, cmd: str, args: Optional[dict[str, Any]] = None) -> None:
    payload: dict[str, Any] = {"request": request_id, "cmd": cmd}
    if args is not None:
        payload["args"] = args
    ws.send(json.dumps(payload, ensure_ascii=True))


def _await_response(ws: Any, request_id: int, log: Callable[[str], None], max_skip: int = 10) -> dict[str, Any]:
    """Read messages until the response matching request_id is found.

    Non-response messages (connection greeting, stray publish_data, ...) can
    arrive before the command reply and are skipped.
    """
    for _ in range(max_skip):
        message = json.loads(ws.recv())
        if message.get("response") == request_id:
            return message
        log(f"[VAR] skipped non-response msg while waiting for id={request_id}: {json.dumps(message)[:200]}")
    raise TimeoutError(f"no response for request id={request_id} after {max_skip} messages")


def run_variable_ws(
    api_ip: str,
    access_token: str,
    local_url: str,
    stop: threading.Event,
    variable_names: Optional[list[str]] = None,
    cycle_time_ms: float = CYCLE_TIME_MS,
    log_fn: Optional[Callable[[str], None]] = None,
) -> None:
    log = log_fn or (lambda _msg: None)

    if websocket is None:
        log("[VAR] websocket-client is not installed - pip install websocket-client")
        return
    if not api_ip or not access_token:
        log(f"[VAR] missing api_ip={api_ip!r} or access_token (len={len(access_token or '')})")
        return

    names = variable_names or DEFAULT_VARIABLES
    url = f"ws://{api_ip}{VARIABLE_SERVICE_PATH}?auth_token={access_token}"
    effective_cycle_ms = max(cycle_time_ms, MIN_CYCLE_TIME_MS)
    if effective_cycle_ms != cycle_time_ms:
        log(f"[VAR] cycle_time_ms={cycle_time_ms} is below controller minimum ({MIN_CYCLE_TIME_MS}); using {effective_cycle_ms}")

    while not stop.is_set():
        ws = None
        try:
            log(f"[VAR] connecting url={url[:url.index('auth_token=')+20]}...")
            ws = websocket.create_connection(url, timeout=5)
            ws.settimeout(5)
            request_id = 1

            _send_command(ws, request_id, "create_topic", {"name": TOPIC_NAME})
            resp = _await_response(ws, request_id, log)
            log(f"[VAR] create_topic response status={resp.get('status')} error={resp.get('error')}")
            if resp.get("status") not in (200, 409):  # 409: topic already exists, harmless
                raise RuntimeError(f"create_topic failed: {resp.get('error')}")

            request_id += 1
            _send_command(ws, request_id, "add_to_topic", {
                "name": TOPIC_NAME,
                "add_not_found_vars": True,
                "vars": [{"name": name, "on_change": False} for name in names],
            })
            resp = _await_response(ws, request_id, log)
            log(f"[VAR] add_to_topic response status={resp.get('status')} result={resp.get('result')} error={resp.get('error')}")
            if resp.get("status") not in (200, 207):
                raise RuntimeError(f"add_to_topic failed: {resp.get('error')}")

            request_id += 1
            _send_command(ws, request_id, "subscribe_topic", {
                "name": TOPIC_NAME,
                "cycle_time_ms": effective_cycle_ms,
                "protocol": "default",
            })
            resp = _await_response(ws, request_id, log)
            log(f"[VAR] subscribe_topic response status={resp.get('status')} error={resp.get('error')}")
            if resp.get("status") != 200:
                raise RuntimeError(f"subscribe_topic failed: {resp.get('error')}")

            message_count = 0
            while not stop.is_set():
                message = json.loads(ws.recv())
                if message.get("topic") == TOPIC_NAME:
                    message_count += 1
                    if message_count == 1:
                        log(f"[VAR] first publish_data received: {json.dumps(message)[:300]}")
                    _post_json(local_url, {"data": message.get("data")})
        except Exception as exc:
            log(f"[VAR] error {type(exc).__name__}: {exc}")
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass
        if not stop.is_set():
            time.sleep(2)


def write_variable(
    api_ip: str,
    access_token: str,
    name: str,
    value: Any,
    var_type: Optional[str] = None,
    log_fn: Optional[Callable[[str], None]] = None,
    timeout_s: float = 5.0,
) -> dict[str, Any]:
    """Write a single variable via a short-lived write_vars command."""
    log = log_fn or (lambda _msg: None)

    if websocket is None:
        return {"ok": False, "error": "websocket-client not installed"}
    if not api_ip or not access_token:
        return {"ok": False, "error": "missing api_ip or access_token"}

    url = f"ws://{api_ip}{VARIABLE_SERVICE_PATH}?auth_token={access_token}"
    var_payload: dict[str, Any] = {"name": name, "value": value, "validate": False}
    if var_type:
        var_payload["type"] = var_type

    ws = None
    try:
        ws = websocket.create_connection(url, timeout=timeout_s)
        ws.settimeout(timeout_s)
        request_id = 1
        _send_command(ws, request_id, "write_vars", {"vars": [var_payload]})
        resp = _await_response(ws, request_id, log)
        log(f"[VAR] write_vars response status={resp.get('status')} result={resp.get('result')} error={resp.get('error')}")
        return {
            "ok": resp.get("status") in (200, 207),
            "status": resp.get("status"),
            "result": resp.get("result"),
            "error": resp.get("error"),
        }
    except Exception as exc:
        log(f"[VAR] write_vars error {type(exc).__name__}: {exc}")
        return {"ok": False, "error": str(exc)}
    finally:
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass


def start_variable_ws(
    api_ip: str,
    access_token: str,
    local_base_url: str,
    variable_names: Optional[list[str]] = None,
    cycle_time_ms: float = CYCLE_TIME_MS,
    log_fn: Optional[Callable[[str], None]] = None,
) -> threading.Event:
    stop = threading.Event()
    local_url = local_base_url.rstrip("/") + "/api/variables-notify"
    thread = threading.Thread(
        target=run_variable_ws,
        args=(api_ip, access_token, local_url, stop, variable_names, cycle_time_ms, log_fn),
        daemon=True,
    )
    thread.start()
    return stop
