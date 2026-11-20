# Claude Code fixture sources

The control protocol follows Anthropic's Claude Agent SDK for Python at commit
`a98138b6ed9aafef05ff646965b15de1ec24ceb5`, accessed 2026-11-19.
The fixtures are synthetic. IDs, instruction text, tool names and inputs,
token counts, and transcript schedules are fixture data.

| Fixture | Source and synthetic cases |
| --- | --- |
| `live.jsonl` | [SDK initialize and request/reply framing][query], [SDK permission handling][permission], [SDK interrupt][interrupt], [streaming input][stream], and [headless output][headless]. The unknown reply ID, stale-session frames, duplicate result, invalid permission input, null request, unanswered request, and result-before-ack ordering are invented fault cases. |
| `resume.jsonl` | [SDK control framing][query], [launcher resume flag][transport], [streaming input][stream], and [headless output][headless]. |
| `pending-permission.jsonl` | [SDK control and permission framing][permission], [streaming input][stream], and [headless output][headless]. The fixture stops while the synthetic permission request is unanswered. |
| `malformed.jsonl` | [SDK control framing][query], [streaming input][stream], and [headless output][headless]. The truncated JSON line is an invented fault. |
| `saturation.jsonl` | [SDK control framing][query], [streaming input][stream], and [headless output][headless]. The burst and null-request barrier are invented stress-test cases. |
| `version-in.txt` and `version-out.txt` | The local argument-validation table in [the harness matrix][matrix] records the 2.1.325 format. The 2.1.326 line is an invented out-of-range probe response. |

The transcript directives `expect`, `send`, `raw`, `repeat`, `wait`, `mark`,
and `hold` belong to the fixture runner, along with the `$index` substitution.

[query]: https://github.com/anthropics/claude-agent-sdk-python/blob/a98138b6ed9aafef05ff646965b15de1ec24ceb5/src/claude_agent_sdk/_internal/query.py#L376
[permission]: https://github.com/anthropics/claude-agent-sdk-python/blob/a98138b6ed9aafef05ff646965b15de1ec24ceb5/src/claude_agent_sdk/_internal/query.py#L640
[interrupt]: https://github.com/anthropics/claude-agent-sdk-python/blob/a98138b6ed9aafef05ff646965b15de1ec24ceb5/src/claude_agent_sdk/_internal/query.py#L847
[transport]: https://github.com/anthropics/claude-agent-sdk-python/blob/a98138b6ed9aafef05ff646965b15de1ec24ceb5/src/claude_agent_sdk/_internal/transport/subprocess_cli.py#L523
[stream]: https://code.claude.com/docs/en/agent-sdk/streaming-vs-single-mode
[headless]: https://code.claude.com/docs/en/headless
[matrix]: ../../../../docs/harness-matrix.md
