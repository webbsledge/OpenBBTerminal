# openbb-agent-server

Pluggable, multi-tenant agent backend that speaks the [OpenBB Workspace
custom-agent SSE protocol][workspace-protocol] and runs the agent loop
on top of the [LangChain DeepAgents harness][deepagents]. One process
hosts many agent profiles; auth, model provider, tools, sub-agents,
middleware, checkpointer, and persistence are independent plugin axes
— anything can be swapped without forking the package.

The full OpenBB Platform — every command across every installed
provider — is reachable via the optional `mcp_local` tool source,
which spawns the sibling [`openbb-mcp-server`](../mcp_server/README.md)
extension over stdio.

The default setup uses 100% free tokens and embedding models available from [NVIDIA](https://build.nvidia.com/) by registering for an API key [here](https://developer.nvidia.com/login)

## Install & run

```bash
pip install -e openbb_platform/extensions/agent_server
# add or combine extras: [openai] [openai_compat]
#                               [bedrock] [vertex] [google_genai]
#                               [groq] [snowflake]

export NVIDIA_API_KEY=...

openbb-agent-server
```

In OpenBB Workspace, add a custom agent pointing at
`http://localhost:8010`. Workspace fetches this once and
reads every agent profile the server registers in a single payload.

For production, generate the config template and edit it:

```bash
openbb-agent-server --generate-config /etc/openbb/openbb.toml
openbb-agent-server --config-file /etc/openbb/openbb.toml --host 0.0.0.0
```

## Documentation

Documentation currently lives in [`docs/`](docs/README.md), and may move in the future:

| Audience                     | Start here                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| First-time user              | [Getting started](docs/guides/getting-started.md) → [Architecture](docs/guides/architecture.md) → [Workspace integration](docs/guides/workspace-integration.md)                                                                                                                                                                                                                                                                                                                                            |
| Operator / SRE               | [Configuration](docs/operating/configuration.md) → [Auth](docs/operating/auth.md) → [Persistence](docs/operating/persistence.md) → [Observability](docs/operating/observability.md)                                                                                                                                                                                                                                                                                                                        |
| Plugin author                | [Plugin system](docs/developing/plugin-system.md) → writing a [tool source](docs/developing/writing-a-tool-source.md) / [model provider](docs/developing/writing-a-model-provider.md) / [middleware](docs/developing/writing-a-middleware.md) / [sub-agent](docs/developing/writing-a-subagent.md) / [auth backend](docs/developing/writing-an-auth-backend.md) → [Conventions](docs/developing/conventions.md) → [Testing](docs/developing/testing.md)                                                     |
| API lookup                   | [Reference](docs/reference/) — module-by-module, mirrors the package tree                                                                                                                                                                                                                                                                                                                                                                                                                                  |
