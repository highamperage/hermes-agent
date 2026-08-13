"""Background watcher to poll tmux 'agy' session for update progress.
Launched by /update command."""

import sys
import time
import subprocess
import os
import json
from hermes_constants import get_hermes_home

def main():
    if len(sys.argv) < 2:
        sys.exit(1)

    tty_path = sys.argv[1]
    task_token = sys.argv[2] if len(sys.argv) > 2 else ""

    def _write(msg):
        try:
            with open(tty_path, "a", encoding="utf-8") as f:
                f.write(msg + "\n")
        except Exception:
            pass

    state_file = os.path.join(get_hermes_home(), "update_task.json")

    time.sleep(1)
    _write("\n  [Watcher] Polling AGY update progress...")

    is_first_capture = True
    seen_lines = set()
    consecutive_errors = 0
    start_time = time.time()
    TIMEOUT = 1800

    expected_done = f"AGY DONE {task_token}"
    expected_failed = f"AGY FAILED {task_token}"

    try:
        while True:
            if time.time() - start_time > TIMEOUT:
                _write("  [Watcher] ✗ Timeout exceeded. Stopping watcher.")
                break

            has_session = subprocess.run(
                ["tmux", "has-session", "-t", "agy"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if has_session.returncode != 0:
                _write("  [Watcher] ✗ tmux session 'agy' disappeared or failed. Update status unknown.")
                break

            cap = subprocess.run(
                ["tmux", "capture-pane", "-p", "-t", "agy"],
                capture_output=True
            )

            if cap.returncode != 0:
                consecutive_errors += 1
                if consecutive_errors > 3:
                    _write("  [Watcher] ✗ Failed to capture tmux pane. Stopping watcher.")
                    break
                time.sleep(2)
                continue

            consecutive_errors = 0
            lines = cap.stdout.decode("utf-8", errors="replace").splitlines()

            if is_first_capture:
                # Establish baseline without printing historical lines
                for line in lines:
                    sline = line.strip()
                    if not sline: continue
                    seen_lines.add(sline)
                    # If sentinel already exists, accept as immediate completion
                    if sline == expected_done:
                        _write("  [Watcher] ✓ Update workflow completed.")
                        return
                    if sline == expected_failed:
                        _write("  [Watcher] ✗ Update workflow failed.")
                        return
                is_first_capture = False
                time.sleep(2)
                continue

            for line in lines:
                sline = line.strip()
                if not sline or sline in seen_lines:
                    continue

                seen_lines.add(sline)

                # Exclude the very long prompt line
                if "Execute self-contained update workflow in" in sline:
                    continue

                if sline == expected_done:
                    _write(f"  [AGY] {sline}")
                    _write("  [Watcher] ✓ Update workflow completed.")
                    return
                if sline == expected_failed:
                    _write(f"  [AGY] {sline}")
                    _write("  [Watcher] ✗ Update workflow failed.")
                    return

                _write(f"  [AGY] {sline}")

            time.sleep(2)
    except Exception as e:
        _write(f"  [Watcher] ✗ Unexpected error: {e}")
    finally:
        try:
            if os.path.exists(state_file):
                with open(state_file, "r") as f:
                    state = json.load(f)
                if state.get("task_token") == task_token:
                    packet_path = state.get("packet_path")
                    if packet_path and os.path.exists(packet_path):
                        try:
                            os.remove(packet_path)
                        except Exception:
                            pass
                    os.remove(state_file)
        except Exception:
            pass

if __name__ == "__main__":
    main()
