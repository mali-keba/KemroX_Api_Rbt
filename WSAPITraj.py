#!/usr/bin/env python3
"""
WSAPITraj
Executed by WSAPIClient to process trajectory selection and trajectory start.
"""

import argparse
import csv
from datetime import datetime
import json
import math
import os
from pathlib import Path
import re
import sys
import time
from typing import Any

try:
    import websocket as _websocket_lib  # type: ignore[reportMissingImports]
    _HAS_WEBSOCKET = True
except ImportError:
    _HAS_WEBSOCKET = False


_PERSISTENT_TRAJ_WS: dict[str, Any] = {}
LOG_FILE = Path(__file__).resolve().parent / "log.json"


def close_persistent_trajectory_client(robot: str) -> None:
    ws = _PERSISTENT_TRAJ_WS.pop(str(robot), None)
    if ws is not None:
        try:
            ws.close()
        except Exception:
            pass


def close_all_persistent_trajectory_clients() -> None:
    for robot in list(_PERSISTENT_TRAJ_WS.keys()):
        close_persistent_trajectory_client(robot)


def start_trajectory_with_persistent_client(
    robot: str,
    csv_path: Path,
    api_ip: str,
    access_token: str,
    overlap_percent: float,
    start_execution: bool = True,
) -> dict:
    return _start_trajectory(
        robot=robot,
        csv_path=csv_path,
        api_ip=api_ip,
        access_token=access_token,
        overlap_percent=overlap_percent,
        start_execution=start_execution,
        keep_ws_open=True,
    )


def start_trajectory_with_shared_ws(
    robot: str,
    csv_path: Path,
    ws: Any,
    overlap_percent: float,
    api_ip: str = "",
    access_token: str = "",
    start_execution: bool = True,
) -> dict:
    return _start_trajectory(
        robot=robot,
        csv_path=csv_path,
        api_ip=api_ip,
        access_token=access_token,
        overlap_percent=overlap_percent,
        start_execution=start_execution,
        keep_ws_open=False,
        existing_ws=ws,
    )


def _robot_suffix(robot_name: str) -> str:
    n = robot_name.lower()
    if "2" in n or "r2" in n:
        return "r2"
    return "r1"


def _append_robot_log(robot: str, event: str, payload: dict) -> None:
    line = {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "event": event,
        "robot": robot,
        "payload": payload,
    }
    try:
        serialized_line = json.dumps(line, ensure_ascii=True) + "\n"
        with LOG_FILE.open("a", encoding="utf-8") as handle:
            handle.write(serialized_line)
    except Exception:
        pass


def _robot_request_id_path(robot: str) -> Path:
    out_dir = Path(__file__).resolve().parent / "TempTraj"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"request_id_{_robot_suffix(robot)}.txt"


def _next_robot_request_id(robot: str) -> int:
    path = _robot_request_id_path(robot)
    current = 1000
    try:
        if path.exists():
            current = int(path.read_text(encoding="utf-8").strip() or "1000")
    except Exception:
        current = 1000

    next_id = current + 1
    try:
        path.write_text(str(next_id), encoding="utf-8")
    except Exception:
        pass
    return next_id


def _reset_robot_log(robot: str) -> None:
    return


def _mask_ws_url_token(ws_url: str) -> str:
    marker = "auth_token="
    idx = ws_url.find(marker)
    if idx < 0:
        return ws_url
    start = idx + len(marker)
    end = ws_url.find("&", start)
    if end < 0:
        end = len(ws_url)
    token = ws_url[start:end]
    if len(token) <= 10:
        masked = "***"
    else:
        masked = token[:6] + "..." + token[-4:]
    return ws_url[:start] + masked + ws_url[end:]


def _should_log_command_traffic(cmd_name: str) -> bool:
    # Keep trajectory log readable: internal status polling is frequent.
    return str(cmd_name) not in {"get_robot_status"}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a trajectory selection for a robot")
    parser.add_argument("--start", action="store_true", help="Start trajectory over robot websocket")
    parser.add_argument("--queue-only", action="store_true", help="Queue trajectory points without starting execution")
    parser.add_argument("--robot", required=True, help="Robot name")
    parser.add_argument("--trajectory", required=True, help="Trajectory CSV absolute path")
    parser.add_argument("--api-ip", default="", help="Robot API IP")
    parser.add_argument("--access-token", default="", help="Robot access token")
    parser.add_argument("--tcp-pos", default="{}", help="Direct trajectory offset as JSON object")
    parser.add_argument("--overlap-percent", default="100", help="Relative overlap percent [0..200]")
    return parser


def _parse_float(value: str) -> float | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    txt = txt.replace(" ", "").replace(",", ".")
    try:
        return float(txt)
    except ValueError:
        return None


def _fmt_float(value: float, use_comma: bool) -> str:
    txt = f"{value:.3f}"
    return txt.replace(".", ",") if use_comma else txt


def _norm_header_name(raw: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(raw).lower())


def _detect_pose_columns(header: list[str]) -> dict[str, int]:
    idx_map: dict[str, int] = {}
    for i, col in enumerate(header):
        n = _norm_header_name(col)
        if n in ("txmm", "tx", "x") and "x" not in idx_map:
            idx_map["x"] = i
        elif n in ("tymm", "ty", "y") and "y" not in idx_map:
            idx_map["y"] = i
        elif n in ("tzmm", "tz", "z") and "z" not in idx_map:
            idx_map["z"] = i
        elif n in ("rxdeg", "rx", "a") and "a" not in idx_map:
            idx_map["a"] = i
        elif n in ("rydeg", "ry", "b") and "b" not in idx_map:
            idx_map["b"] = i
        elif n in ("rzdeg", "rz", "c") and "c" not in idx_map:
            idx_map["c"] = i
    return idx_map


def _detect_joint_columns(header: list[str]) -> dict[str, int]:
    idx_map: dict[str, int] = {}
    for i, col in enumerate(header):
        n = _norm_header_name(col)
        if n in ("a1", "axis1", "j1", "joint1") and "a1" not in idx_map:
            idx_map["a1"] = i
        elif n in ("a2", "axis2", "j2", "joint2") and "a2" not in idx_map:
            idx_map["a2"] = i
        elif n in ("a3", "axis3", "j3", "joint3") and "a3" not in idx_map:
            idx_map["a3"] = i
        elif n in ("a4", "axis4", "j4", "joint4") and "a4" not in idx_map:
            idx_map["a4"] = i
        elif n in ("a5", "axis5", "j5", "joint5") and "a5" not in idx_map:
            idx_map["a5"] = i
        elif n in ("a6", "axis6", "j6", "joint6") and "a6" not in idx_map:
            idx_map["a6"] = i
    return idx_map


def _read_cartesian_points(csv_path: Path) -> list[dict]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle, delimiter=";")
        rows = list(reader)

    if not rows:
        raise ValueError("CSV vide")

    header = rows[0]
    data_rows = [r for r in rows[1:] if any(str(c).strip() for c in r)]
    if not data_rows:
        raise ValueError("CSV sans points de trajectoire")

    pose_cols = _detect_pose_columns(header)
    required = {"x", "y", "z", "a", "b", "c"}
    missing = sorted(list(required - set(pose_cols.keys())))
    if missing:
        raise ValueError(f"Colonnes pose introuvables: {', '.join(missing)}")

    points: list[dict] = []
    for row in data_rows:
        pos = {}
        valid = True
        for axis in ("x", "y", "z", "a", "b", "c"):
            idx = pose_cols[axis]
            if idx >= len(row):
                valid = False
                break
            v = _parse_float(row[idx])
            if v is None:
                valid = False
                break
            pos[axis] = float(v)

        if valid:
            points.append({"pos": pos})

    if not points:
        raise ValueError("Aucun point valide trouve")

    return points


def _read_joint_points(csv_path: Path) -> list[dict]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle, delimiter=";")
        rows = list(reader)

    if not rows:
        raise ValueError("CSV vide")

    header = rows[0]
    data_rows = [r for r in rows[1:] if any(str(c).strip() for c in r)]
    if not data_rows:
        raise ValueError("CSV sans points de trajectoire")

    joint_cols = _detect_joint_columns(header)
    required = {"a1", "a2", "a3", "a4", "a5", "a6"}
    missing = sorted(list(required - set(joint_cols.keys())))
    if missing:
        raise ValueError(f"Colonnes axes introuvables: {', '.join(missing)}")

    points: list[dict] = []
    for row in data_rows:
        joints: list[float] = []
        valid = True
        for axis in ("a1", "a2", "a3", "a4", "a5", "a6"):
            idx = joint_cols[axis]
            if idx >= len(row):
                valid = False
                break
            v = _parse_float(row[idx])
            if v is None:
                valid = False
                break
            joints.append(float(v))

        if valid:
            points.append({"joints": joints})

    if not points:
        raise ValueError("Aucun point axes valide trouve")

    return points


def _read_trajectory_points(csv_path: Path) -> tuple[str, list[dict]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle, delimiter=";")
        rows = list(reader)

    if not rows:
        raise ValueError("CSV vide")

    header = rows[0]
    pose_cols = _detect_pose_columns(header)
    joint_cols = _detect_joint_columns(header)

    if {"x", "y", "z", "a", "b", "c"}.issubset(set(pose_cols.keys())):
        return "cartesian", _read_cartesian_points(csv_path)

    if {"a1", "a2", "a3", "a4", "a5", "a6"}.issubset(set(joint_cols.keys())):
        return "joints", _read_joint_points(csv_path)

    raise ValueError("Format CSV non supporte: attendu colonnes pose (Tx..Rz) ou axes (A1..A6)")


def _compute_translation_dynamic(points: list[dict]) -> list[dict]:
    return [{
        "vel": 250.0,
        "acc": 2500.0,
        "dec": 2500.0,
        "jrk": 2500.0,
    } for _ in points]


def _wait_ws_response(ws, robot: str, expected_request_id: int, max_reads: int = 1000, wait_timeout_s: float = 10.0) -> dict:
    last_unmatched = None
    deadline = time.perf_counter() + max(0.1, wait_timeout_s)
    reads = 0
    while time.perf_counter() < deadline and reads < max_reads:
        try:
            raw = ws.recv()
        except Exception as exc:
            if "timed out" in str(exc).lower() or "timeout" in str(exc).lower():
                continue
            _append_robot_log(robot, "ws_error", {
                "message": "ws recv failed while waiting response",
                "expected_request_id": expected_request_id,
                "error": str(exc),
            })
            raise
        reads += 1
        resp = json.loads(raw)
        if "response" not in resp:
            continue

        response_value = resp.get("response")
        if response_value != expected_request_id:
            status_value = int(resp.get("status", 0) or 0)
            err = resp.get("error") if isinstance(resp.get("error"), dict) else {}
            err_key = str(err.get("key", ""))

            # With async path queueing, successful ACKs for previous requests
            # can arrive while waiting for the next response. This is expected.
            if status_value == 200:
                _append_robot_log(robot, "ws_info", {
                    "message": "Out-of-order successful response ignored",
                    "expected_request_id": expected_request_id,
                    "response_id": response_value,
                    "raw": resp,
                })
                continue

            # After clear_path, old queued path commands can legitimately return cmd_aborted.
            if status_value == 900 and err_key == "cmd_aborted":
                _append_robot_log(robot, "ws_info", {
                    "message": "Stale command aborted after clear_path",
                    "expected_request_id": expected_request_id,
                    "response_id": response_value,
                    "raw": resp,
                })
                continue

            last_unmatched = {
                "message": "Unexpected response value",
                "expected_request_id": expected_request_id,
                "response_id": response_value,
                "raw": resp,
            }
            _append_robot_log(robot, "ws_error", last_unmatched)
            continue

        return resp

    if last_unmatched is None:
        last_unmatched = {
            "message": "No matching command response received",
            "expected_request_id": expected_request_id,
            "wait_timeout_s": wait_timeout_s,
        }
    _append_robot_log(robot, "ws_error", last_unmatched)
    raise RuntimeError(f"No matching response for request {expected_request_id}")


def _start_trajectory(
    robot: str,
    csv_path: Path,
    api_ip: str,
    access_token: str,
    overlap_percent: float,
    start_execution: bool = True,
    keep_ws_open: bool = False,
    existing_ws: Any = None,
) -> dict:
    if not _HAS_WEBSOCKET:
        raise RuntimeError("websocket-client not installed. Run: pip install websocket-client")
    if not api_ip:
        raise RuntimeError("Missing api_ip for websocket start")
    if not access_token:
        raise RuntimeError("Missing access token for websocket start")

    ws_url = f"ws://{api_ip}/api/v4/robot-control/robots/{robot}/websocket-command?auth_token={access_token}" if api_ip and access_token else "shared_ws"

    _reset_robot_log(robot)

    try:
        trajectory_mode, points = _read_trajectory_points(csv_path)
    except Exception as exc:
        _append_robot_log(robot, "start_error", {
            "stage": "trajectory_preflight",
            "trajectory": str(csv_path),
            "error": str(exc),
        })
        raise

    path_command = "path_lin" if trajectory_mode == "cartesian" else "path_ptp"
    _append_robot_log(robot, "start_begin", {
        "trajectory": str(csv_path),
        "overlap_percent": overlap_percent,
        "mode": path_command,
        "start_execution": start_execution,
        "ws_url": _mask_ws_url_token(ws_url),
    })

    _append_robot_log(robot, "start_points_loaded", {
        "trajectory": str(csv_path),
        "points_count": len(points),
    })

    axes_percent = 100

    def _open_ws_with_retry(max_attempts: int = 3):
        last_exc: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                return _websocket_lib.create_connection(ws_url, timeout=5)
            except Exception as exc:
                last_exc = exc
                _append_robot_log(robot, "ws_info", {
                    "message": "websocket connect retry",
                    "attempt": attempt,
                    "error": str(exc),
                })
                time.sleep(0.2)
        raise RuntimeError(f"websocket connect failed: {last_exc}")

    owns_ws = False
    if existing_ws is not None:
        ws = existing_ws
    elif keep_ws_open:
        ws = _PERSISTENT_TRAJ_WS.get(robot)
        if ws is None:
            ws = _open_ws_with_retry()
            _PERSISTENT_TRAJ_WS[robot] = ws
    else:
        ws = _open_ws_with_retry()
        owns_ws = True
    sent = 0
    cmd_seq = 0

    def _send_command(
        cmd_name: str,
        args: dict | None = None,
        wait_response: bool = True,
        wait_timeout_s: float = 10.0,
        raise_on_error: bool = True,
    ) -> tuple[int, dict | None]:
        nonlocal cmd_seq
        cmd_seq += 1
        req_id = _next_robot_request_id(robot)
        payload = {
            "request": req_id,
            "cmd": cmd_name,
        }
        if args is not None:
            payload["args"] = args

        if _should_log_command_traffic(cmd_name):
            _append_robot_log(robot, "cmd_send", {
                "seq": cmd_seq,
                "request": req_id,
                "cmd": cmd_name,
                "args": args,
                "wait_response": wait_response,
            })
        sent_at = time.perf_counter()
        ws.send(json.dumps(payload, ensure_ascii=True))

        if not wait_response:
            return req_id, None

        resp = _wait_ws_response(ws, robot, req_id, wait_timeout_s=wait_timeout_s)
        response_time_ms = round((time.perf_counter() - sent_at) * 1000.0, 3)
        status = int(resp.get("status", 0))
        if _should_log_command_traffic(cmd_name):
            _append_robot_log(robot, f"cmd_response: {response_time_ms}", {
                "seq": cmd_seq,
                "request": req_id,
                "cmd": cmd_name,
                "status": status,
                "response": resp,
            })
        if status != 200 and raise_on_error:
            err = resp.get("error", {})
            _append_robot_log(robot, "ws_error", {
                "message": f"{cmd_name} failed",
                "status": status,
                "error": err,
                "expected_request_id": req_id,
                "response_id": resp.get("response"),
            })
            raise RuntimeError(f"{cmd_name} failed: status={status} error={err}")
        return req_id, resp

    def _read_robot_status_snapshot() -> dict | None:
        try:
            _, st_resp = _send_command(
                "get_robot_status",
                wait_response=True,
                wait_timeout_s=5.0,
                raise_on_error=False,
            )
            if isinstance(st_resp, dict):
                result = st_resp.get("result")
                if isinstance(result, dict):
                    return result
        except Exception:
            pass
        return None

    def _nudge_wrist_pitch_if_needed(args: dict) -> dict:
        patched = json.loads(json.dumps(args))
        try:
            pos = patched["position"]["cartesian"]["pos"]
            b = float(pos.get("b", 0.0))
        except Exception:
            return patched

        # Keep away from +/-90deg where Euler representation is wrist-singular.
        if abs(abs(b) - 90.0) < 2.0:
            if b >= 0:
                pos["b"] = 92.5 if b >= 90.0 else 87.5
            else:
                pos["b"] = -92.5 if b <= -90.0 else -87.5
        return patched

    def _send_path_with_retry(args: dict, max_attempts: int = 6, wait_response: bool = True) -> None:
        last_error: dict | None = None
        current_args = _nudge_wrist_pitch_if_needed(args)
        for attempt in range(1, max_attempts + 1):
            _, resp = _send_command(
                path_command,
                current_args,
                wait_response=wait_response,
                wait_timeout_s=15.0,
                raise_on_error=False,
            )
            if not wait_response:
                return
            if not isinstance(resp, dict):
                time.sleep(0.05)
                continue

            status = int(resp.get("status", 0))
            if status == 200:
                return

            err = resp.get("error", {}) if isinstance(resp.get("error"), dict) else {}
            err_txt = json.dumps(err, ensure_ascii=True).lower()
            err_key = str(err.get("key", "")).lower()
            last_error = {"status": status, "error": err}

            _append_robot_log(robot, "ws_info", {
                "message": f"{path_command} retry",
                "attempt": attempt,
                "status": status,
                "error": err,
            })

            if err_key == "buffer_overrun":
                time.sleep(min(0.30, 0.05 * attempt))
                continue

            if "singular" in err_key or "singular" in err_txt:
                current_args = _nudge_wrist_pitch_if_needed(current_args)
                _append_robot_log(robot, "ws_info", {
                    "message": f"{path_command} wrist singularity mitigation",
                    "attempt": attempt,
                    "adjusted_pos": current_args.get("position", {}),
                })
                time.sleep(0.05)
                continue

            break

        raise RuntimeError(f"{path_command} failed after retries: {last_error}")

    def _wait_execution_state_completed(wait_timeout_s: float = 180.0) -> None:
        deadline = time.monotonic() + wait_timeout_s
        last_snapshot = None
        while time.monotonic() < deadline:
            status_snapshot = _read_robot_status_snapshot()
            if isinstance(status_snapshot, dict):
                last_snapshot = status_snapshot
                path_exec_state = str(status_snapshot.get("path_execution_state", "")).lower()
                if path_exec_state in ("idle", "init", "standby"):
                    _append_robot_log(robot, "ws_info", {
                        "message": "post_wait_execution_state",
                        "robot_status": status_snapshot,
                    })
                    return
                if path_exec_state in ("executed", "interrupted"):
                    # Guard against immediate close right after wait response.
                    time.sleep(1.5)
                    final_snapshot = _read_robot_status_snapshot()
                    if isinstance(final_snapshot, dict):
                        _append_robot_log(robot, "ws_info", {
                            "message": "post_wait_execution_state",
                            "robot_status": final_snapshot,
                        })
                    else:
                        _append_robot_log(robot, "ws_info", {
                            "message": "post_wait_execution_state",
                            "robot_status": status_snapshot,
                        })
                    return
            time.sleep(0.1)

        _append_robot_log(robot, "ws_error", {
            "message": "Execution state did not reach completed state before timeout",
            "last_robot_status": last_snapshot,
            "wait_timeout_s": wait_timeout_s,
        })
        raise RuntimeError("Execution did not reach completed state before timeout")

    try:
        active_client_ready = False
        if start_execution:
            for attempt in range(1, 4):
                try:
                    _send_command("set_active_client")
                    active_client_ready = True
                    break
                except Exception as exc:
                    _append_robot_log(robot, "ws_info", {
                        "message": "set_active_client retry",
                        "attempt": attempt,
                        "error": str(exc),
                    })
                    try:
                        ws.close()
                    except Exception:
                        pass
                    if attempt >= 3:
                        raise
                    if existing_ws is not None:
                        raise RuntimeError("set_active_client failed on shared websocket")
                    time.sleep(0.2)
                    ws = _open_ws_with_retry(max_attempts=2)
                    if keep_ws_open:
                        _PERSISTENT_TRAJ_WS[robot] = ws
        else:
            active_client_ready = True

        if not active_client_ready:
            raise RuntimeError("set_active_client failed after retries")

        if start_execution:
            _send_command("start_path_execution", wait_timeout_s=10.0)

        send_points = points[1:] if len(points) > 1 else []
        if not send_points:
            if points:
                send_points = [points[0]]
                _append_robot_log(robot, "ws_info", {
                    "message": "single-point trajectory fallback enabled",
                    "trajectory_mode": trajectory_mode,
                })
            else:
                raise RuntimeError("Trajectory has no executable points after removing the zero-offset first point.")

        for i, point in enumerate(send_points):
            translation = {
                "vel": 250.0,
                "acc": 2500.0,
                "dec": 2500.0,
                "jrk": 2500.0,
            }

            dynamic_values = {
                "axes": {
                    "vel": axes_percent,
                    "acc": axes_percent,
                    "dec": axes_percent,
                    "jrk": axes_percent,
                },
                # Some controllers validate these members even for joint motion.
                "translation": {
                    "vel": 250.0,
                    "acc": 2500.0,
                    "dec": 2500.0,
                    "jrk": 2500.0,
                },
                "rotation": {
                    "vel": 90.0,
                    "acc": 180.0,
                    "dec": 180.0,
                    "jrk": 180.0,
                },
            }
            if trajectory_mode == "cartesian":
                dynamic_values["translation"] = translation

            if trajectory_mode == "cartesian":
                position_payload = {
                    "cartesian": {
                        "pos": {
                            "x": point["pos"]["x"],
                            "y": point["pos"]["y"],
                            "z": point["pos"]["z"],
                            "a": point["pos"]["a"],
                            "b": point["pos"]["b"],
                            "c": point["pos"]["c"],
                        }
                    }
                }
            else:
                position_payload = {
                    "joints": point["joints"],
                }

            args = {
                "position": position_payload,
                "dynamic": {
                    "values": dynamic_values,
                },
                "overlap": {
                    "relative": {
                        "percent": 200.0
                    }
                },
                "cmd_name": f"traj_{i + 1}",
            }

            _send_path_with_retry(args, wait_response=True)
            sent += 1

        if not start_execution:
            _append_robot_log(robot, "points_queued", {
                "points_sent": sent,
            })
            return {
                "points_sent": sent,
                "execution_started": False,
            }

        wait_req_id, _ = _send_command("path_wait_is_finished", {"cmd_name": "traj_wait"}, wait_response=False)

        wait_resp = _wait_ws_response(ws, robot, wait_req_id, max_reads=5000, wait_timeout_s=180.0)
        wait_status = int(wait_resp.get("status", 0))
        if wait_status != 200:
            err = wait_resp.get("error", {})
            _append_robot_log(robot, "ws_error", {
                "message": "path_wait_is_finished failed",
                "status": wait_status,
                "error": err,
                "expected_request_id": wait_req_id,
                "response_id": wait_resp.get("response"),
            })
            raise RuntimeError(f"path_wait_is_finished failed: status={wait_status} error={err}")

        _wait_execution_state_completed(wait_timeout_s=300.0)
    finally:
        if keep_ws_open and existing_ws is None:
            _PERSISTENT_TRAJ_WS[robot] = ws
        elif owns_ws:
            try:
                ws.close()
            except Exception:
                pass

    _append_robot_log(robot, "start_end", {
        "points_sent": sent,
    })

    return {
        "points_sent": sent,
        "overlap_percent": overlap_percent,
    }


def _parse_tcp_pos_arg(value: str) -> dict:
    raw = str(value or "").strip()
    if not raw:
        return {}

    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        pass

    cleaned = raw.strip("{} ")
    if not cleaned:
        return {}

    result: dict[str, float] = {}
    for part in cleaned.split(","):
        if ":" not in part:
            continue
        k, v = part.split(":", 1)
        key = k.strip().strip('"\'').lower()
        num = _parse_float(v)
        if key and num is not None:
            result[key] = num
    return result


def _normalize_tcp_pos_map(data: dict) -> dict[str, float]:
    # Accept both flat and nested TCP payload shapes.
    if not isinstance(data, dict):
        return {}

    candidates = [data]
    pos = data.get("pos")
    if isinstance(pos, dict):
        candidates.append(pos)

    position = data.get("position")
    if isinstance(position, dict):
        cart = position.get("cartesian")
        if isinstance(cart, dict):
            cpos = cart.get("pos")
            if isinstance(cpos, dict):
                candidates.append(cpos)

    axes = ("x", "y", "z", "a", "b", "c")
    for candidate in candidates:
        normalized: dict[str, float] = {}
        for axis in axes:
            raw = candidate.get(axis)
            if isinstance(raw, (int, float)):
                normalized[axis] = float(raw)
                continue
            parsed = _parse_float(str(raw))
            if parsed is None:
                break
            normalized[axis] = parsed
        if len(normalized) == len(axes):
            return normalized

    return {}


def _generate_offset_trajectory(csv_path: Path, robot: str, tcp_pos: dict) -> dict:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle, delimiter=";")
        rows = list(reader)

    if not rows:
        raise ValueError("CSV vide")

    header = rows[0]
    data_rows = [r for r in rows[1:] if any(str(c).strip() for c in r)]
    if not data_rows:
        raise ValueError("CSV sans points de trajectoire")

    pose_cols = _detect_pose_columns(header)
    has_cartesian = {"x", "y", "z", "a", "b", "c"}.issubset(set(pose_cols.keys()))

    tcp_pos = _normalize_tcp_pos_map(tcp_pos)

    offset = {}
    for axis in ("x", "y", "z", "a", "b", "c"):
        raw = tcp_pos.get(axis)
        if isinstance(raw, (int, float)):
            offset[axis] = float(raw)
        else:
            parsed = _parse_float(str(raw))
            offset[axis] = parsed if parsed is not None else 0.0

    use_comma = False
    if has_cartesian:
        first = data_rows[0]
        use_comma = any(
            "," in str(first[pose_cols[a]])
            for a in ("x", "y", "z", "a", "b", "c")
            if pose_cols[a] < len(first)
        )

    transformed_rows = [header]
    for row in data_rows:
        out = list(row)
        if has_cartesian:
            for axis, idx in pose_cols.items():
                if idx >= len(out):
                    continue
                val = _parse_float(out[idx])
                if val is None:
                    continue
                out[idx] = _fmt_float(val + offset[axis], use_comma)

        transformed_rows.append(out)

    out_name = f"temptraj_{_robot_suffix(robot)}.csv"
    out_dir = Path(__file__).resolve().parent / "TempTraj"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / out_name
    with out_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=";", lineterminator="\n")
        writer.writerows(transformed_rows)

    _append_robot_log(robot, "temptraj_generated", {
        "source": str(csv_path),
        "output": str(out_path),
        "points_count": len(data_rows),
        "offset": {k: round(v, 6) for k, v in offset.items()},
    })

    return {
        "output_file": out_name,
        "output_path": str(out_path),
        "points_count": len(data_rows),
        "offset": {k: round(v, 6) for k, v in offset.items()},
    }


def main() -> int:
    try:
        args = _build_parser().parse_args()
    except SystemExit as exc:
        try:
            return int(exc.code)
        except (TypeError, ValueError):
            return 1

    trajectory_path = Path(args.trajectory)
    tcp_pos = _parse_tcp_pos_arg(args.tcp_pos)
    if not isinstance(tcp_pos, dict):
        tcp_pos = {}

    try:
        overlap_percent = float(args.overlap_percent)
    except (TypeError, ValueError):
        overlap_percent = 100.0
    overlap_percent = max(0.0, min(200.0, overlap_percent))

    if not trajectory_path.exists() or not trajectory_path.is_file():
        print(json.dumps({
            "ok": False,
            "status_code": 404,
            "response": {
                "message": "Trajectory file not found",
                "trajectory": str(trajectory_path),
            },
        }, ensure_ascii=True))
        return 1

    if args.start:
        try:
            result_info = _start_trajectory(
                robot=args.robot,
                csv_path=trajectory_path,
                api_ip=args.api_ip,
                access_token=args.access_token,
                overlap_percent=overlap_percent,
                start_execution=not args.queue_only,
            )
        except Exception as exc:
            print(json.dumps({
                "ok": False,
                "status_code": 500,
                "response": {
                    "message": f"Unable to start trajectory: {exc}",
                    "trajectory": str(trajectory_path),
                },
            }, ensure_ascii=True))
            return 1

        print(json.dumps({
            "ok": True,
            "status_code": 200,
            "response": {
                "message": "Trajectory points queued" if args.queue_only else "Trajectory start completed",
                "robot": args.robot,
                "trajectory": trajectory_path.name,
                **result_info,
            },
        }, ensure_ascii=True))
        return 0

    try:
        result_info = _generate_offset_trajectory(trajectory_path, args.robot, tcp_pos)
    except Exception as exc:
        print(json.dumps({
            "ok": False,
            "status_code": 500,
            "response": {
                "message": f"Unable to build temp trajectory CSV: {exc}",
                "trajectory": str(trajectory_path),
            },
        }, ensure_ascii=True))
        return 1

    print(json.dumps({
        "ok": True,
        "status_code": 200,
        "response": {
            "message": "Temp trajectory generated",
            "robot": args.robot,
            "trajectory": trajectory_path.name,
            "points_count": result_info["points_count"],
            "output_file": result_info["output_file"],
            "offset": result_info["offset"],
            "api_ip": args.api_ip,
            "token_present": bool(args.access_token),
        },
    }, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    exit_code = main()
    in_vscode_debug = bool(os.environ.get("DEBUGPY_LAUNCHER_PORT"))
    if sys.gettrace() is None and not in_vscode_debug:
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        except Exception:
            pass
        os._exit(int(exit_code))
