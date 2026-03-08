```markdown
# Terminal Synchronization Bypass & Failsafes
### Configuration Variables
To adjust the behavior of the terminal bypass rules, modify the following variables:
*   **`[VAR_TEMP_DIR]`**: `.agents/tmp/` -> The workspace-relative directory to store output files.
*   **`[VAR_FAST_TRACK_MS]`**: `2500` -> Time in milliseconds to wait for a command before sending it to the background. Increase this to allow more commands to finish synchronously and save verification steps.
*   **`[VAR_CLEANUP_DAYS]`**: `+1` -> Number of days after which old terminal logs are deleted.
*   **`[VAR_TIMEOUT_MINS]`**: `2` -> Number of minutes a background command can run before the agent prompts for termination.
---
### Terminal Synchronization Bypass (Mandatory)
To avoid known platform bugs where terminal output hangs or the status gets stuck as `RUNNING`, **ALWAYS** follow these steps for every terminal command:
**Pre-requisites & Cleanup**: Before running your first command in a conversation:
*   Ensure the directory exists: `mkdir -p [VAR_TEMP_DIR]`
*   Keep the workspace clean by deleting old terminal outputs from previous conversations: `find [VAR_TEMP_DIR] -type f -name "ag_output_*.txt" -mtime [VAR_CLEANUP_DAYS] -delete 2>/dev/null || true`
*   *Note: Always read and write to `[VAR_TEMP_DIR]`. This avoids out-of-workspace permission prompts while keeping temporary execution files organized.*
2.  **Mandatory Redirection (with Tee)**: Append `2>&1 | tee [VAR_TEMP_DIR]ag_output_<conversation_id>.txt && echo "===AGENT COMMAND DONE===" >> [VAR_TEMP_DIR]ag_output_<conversation_id>.txt` to every `run_command` call.
*   *Why? `tee` (without `-a`) overwrites the file for each new command within the same conversation, keeping the file small. It ensures the output is visible to the user in the IDE terminal while also writing it securely to the file for the agent to verify.*
3.  **Wait Injection**: Set the `WaitMsBeforeAsync` parameter of the `run_command` tool to exactly `[VAR_FAST_TRACK_MS]`.
### Completion Logic & Optimization (Mandatory)
*   **Synchronous Fast-Track**: If `run_command` completes within the `[VAR_FAST_TRACK_MS]` window, it will return the output directly (no Background command ID is generated). You may use the output normally and **SKIP** the verification steps below.
*   **Asynchronous Filesystem Verification**: If the command exceeds `[VAR_FAST_TRACK_MS]` and returns a Background command ID, immediately call `view_file` on `[VAR_TEMP_DIR]ag_output_<conversation_id>.txt`.
*   Treat the content of this file as the official command output. If the file contains the "===AGENT COMMAND DONE===" marker, proceed with the task immediately.
*   *Ignore Status Hangs*: Never wait for `command_status` to report `DONE` if `view_file` confirms the command has finished executing. If the command appears stuck in the UI, disregard it and move to the next step.
### Long-Running Commands Failsafe (Mandatory)
To prevent zombie processes or runaway commands from hanging tasks indefinitely, always enforce a strict time limit for command execution:
*   **Time Limit**: If a terminal background command has been running for `[VAR_TIMEOUT_MINS]` minutes without producing expected output or the completion marker, **STOP waiting implicitly**.
*   **Prompt for Action**: Use the `notify_user` tool to explicitly ask the user for confirmation. Example: "The command `XYZ` has been running for over `[VAR_TIMEOUT_MINS]` minutes. Should I keep waiting, or terminate it?"
*   **Termination Tool**: If the user instructs you to terminate it, use the `send_command_input` tool with `Terminate: true` and the corresponding `CommandId` to kill the runaway process cleanly.
```
