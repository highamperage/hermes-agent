"""Background watcher to poll tmux 'agy' session for update progress.
Launched by /update command."""

import sys
import time
import subprocess
import os

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

    time.sleep(1)
    _write("\n  [Watcher] Polling AGY update progress...")
    
    seen_lines = set()
    consecutive_errors = 0
    
    while True:
        try:
            # Check if session exists
            has_session = subprocess.run(
                ["tmux", "has-session", "-t", "agy"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if has_session.returncode != 0:
                _write("  [Watcher] ✗ tmux session 'agy' disappeared or failed. Update status unknown.")
                break

            # Capture pane contents
            cap = subprocess.run(
                ["tmux", "capture-pane", "-p", "-t", "agy"], 
                capture_output=True, 
                text=True
            )
            
            if cap.returncode != 0:
                consecutive_errors += 1
                if consecutive_errors > 3:
                    _write("  [Watcher] ✗ Failed to capture tmux pane. Stopping watcher.")
                    break
                time.sleep(2)
                continue
            
            consecutive_errors = 0
            lines = cap.stdout.splitlines()
            
            for line in lines:
                sline = line.strip()
                if not sline or sline in seen_lines:
                    continue
                
                seen_lines.add(sline)
                
                # Exclude the very long prompt line to avoid spamming the screen
                if "Execute self-contained update workflow in" in sline:
                    continue
                # Skip printing stale completions
                if "AGY DONE" in sline and task_token and task_token not in sline:
                    continue
                
                # Print the line
                _write(f"  [AGY] {sline}")
                
                expected_done = f"AGY DONE {task_token}".strip()
                if expected_done in sline:
                    _write("  [Watcher] ✓ Update workflow completed successfully (AGY DONE).")
                    return

            time.sleep(2)
        except Exception as e:
            _write(f"  [Watcher] ✗ Unexpected error: {e}")
            break

if __name__ == "__main__":
    main()
