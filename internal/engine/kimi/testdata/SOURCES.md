# Kimi Code fixture sources

All wire messages follow Agent Client Protocol schema **v1**
(`protocolVersion: 1`, JSON-RPC 2.0) and
[Kimi Code's ACP method coverage][kimi]. These synthetic transcripts use
invented IDs, paths, instructions, timings, and tool data.

| Fixture | Source and synthetic cases |
| --- | --- |
| `live.jsonl` | [ACP initialization][init], [session setup][session], [configuration options][config], [prompt and cancel][prompt], and [tool calls and permission responses][tools]. `tool_call` and `tool_call_update` use arrays of ACP tool content with nested text blocks. Extra permission round trips and foreign-session updates are invented test cases. |
| `malformed.jsonl` | [ACP initialization][init] and [session setup][session] precede an invented truncated JSON frame. |
| `out-of-order.jsonl` | [ACP session setup][session], [prompt and update frames][prompt], and [tool permissions][tools]. Updates before session creation, replies with unrelated IDs, and the foreign-session update are invented ordering faults. The extra permission exchange acts as a synthetic barrier. |
| `saturation.jsonl` | [ACP initialization][init], [session setup][session], and [agent message updates][prompt]. The repeated burst and interrupted transcript are invented stress-test cases. |

The transcript directives and the `$cwd` substitution belong to the fixture runner.

[kimi]: https://www.kimi.com/code/docs/en/kimi-code-cli/reference/kimi-acp
[init]: https://agentclientprotocol.com/protocol/v1/initialization
[session]: https://agentclientprotocol.com/protocol/v1/session-setup
[config]: https://agentclientprotocol.com/protocol/v1/session-config-options
[prompt]: https://agentclientprotocol.com/protocol/v1/prompt-turn
[tools]: https://agentclientprotocol.com/protocol/v1/tool-calls
