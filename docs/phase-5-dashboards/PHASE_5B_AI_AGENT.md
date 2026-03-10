# Phase 5B: AI Operations Agent

> An autonomous reasoning agent built with LangGraph and Claude that directly queries BigQuery, MongoDB, and Kafka monitoring data. Not a wrapper -- the agent writes SQL, chains tool calls, detects anomalies, generates charts, and maintains conversation memory across sessions.

---

## Why This Exists

Most "AI-powered dashboards" are thin wrappers: take a question, hit an LLM API, return text. This agent is different:

1. **It executes against live infrastructure.** The agent runs real SQL against BigQuery, queries MongoDB collections, and checks Kafka consumer lag. Every answer is backed by actual data.
2. **It reasons in multiple steps.** A single user question can trigger a chain of 3-5 tool calls: inspect a schema, run a diagnostic query, compare against historical averages, then generate a chart.
3. **It remembers context.** Thread-based memory means follow-up questions work naturally.

---

## Architecture

```
User question
     |
     v
React Chat UI (Agent.tsx)
     |  POST /api/agent/chat  {messages, thread_id}
     v
FastAPI SSE Endpoint (routers/agent.py)
     |  Injects SystemMessage on first message per thread
     v
LangGraph StateGraph (agent/graph.py)
     |
     +-- agent_node --> Claude Sonnet 4 (with 11 tools bound)
     |       |
     |       v  (has tool_calls?)
     |   +-- YES --> tools node (ToolNode) --> executes tool --> back to agent_node
     |   +-- NO  --> END
     |
     v
MemorySaver Checkpointer
     |  Persists full message history per thread_id
     v
SSE Event Stream back to React
     |  Events: thinking, tool_call, tool_result, chart, answer, done
     v
React renders reasoning steps + inline charts
```

---

## Core Components

### State Management: AgentState

```python
class AgentState(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]
    tool_call_count: int
```

The `add_messages` reducer is critical. It tells LangGraph to **append** messages instead of overwriting them as the graph cycles through agent -> tools -> agent. Without it, tool results arrive at Claude without their corresponding tool_use blocks, causing API rejection.

### Graph: build_agent()

Two nodes connected in a loop:

- **agent_node** -- Invokes Claude with the full message history and all tools bound. Returns an AIMessage (either with tool_calls or a final text response).
- **tools (ToolNode)** -- Executes whichever tools Claude requested. Returns ToolMessages with results.

Routing: `should_continue` checks if the last AIMessage has tool_calls. If yes, route to tools. If no, route to END.

The graph is compiled once as a singleton with a MemorySaver checkpointer, reused across all requests.

### Memory: MemorySaver

Each conversation thread gets its own checkpointed state. The checkpointer stores the full message history (system prompt, user messages, AI responses, tool calls, tool results) keyed by thread_id.

When a user sends a message on an existing thread, the router checks for existing history. If history exists, only the new user message is appended (no duplicate system prompt). If no history, the system prompt is prepended to the first message.

### System Prompt

The prompt in `api/agent/prompts.py` gives the agent:

- **Platform architecture context** -- BigQuery, MongoDB, Kafka, Dagster roles
- **Complete BigQuery schema** -- Every dataset, table, column name, and approximate row count
- **MongoDB database/collection map** -- Both ecommerce_analytics and ecommerce_streaming
- **All 10 A/B experiments** -- IDs, names, primary metrics
- **Capability list** -- When to use which tool
- **Response guidelines** -- No emojis, show SQL, format numbers, cite evidence

This schema awareness is what makes the agent's SQL generation accurate.

---

## Tools (11 total)

### Read/Query Tools

| Tool | Data Source | Purpose |
|------|-----------|---------|
| `query_bigquery(sql)` | BigQuery | Execute any SELECT query. Write-blocked. Results limited to 50 rows. |
| `query_mongodb(database, collection, filter_doc, limit)` | MongoDB | Query any collection with optional filters. |
| `check_kafka_consumers()` | BigQuery monitoring | Latest snapshot per consumer group: lag, status, partitions. |
| `check_data_freshness()` | BigQuery metadata | Last modified time, row count, and size for 12 key tables. |
| `get_experiment_summary(experiment_id?)` | BigQuery marts | Control vs treatment comparison with p-value, lift, significance. |
| `get_table_schema(dataset, table)` | BigQuery INFORMATION_SCHEMA | Column names, data types, nullability. |

### Analysis Tools

| Tool | Purpose |
|------|---------|
| `run_anomaly_scan()` | Scans GMV, order volume, cancellation rate, delivery time, review scores, and data freshness against 30-day moving averages using z-scores. Returns findings with severity levels. |
| `generate_chart(chart_type, sql, title, x_key, y_keys)` | Executes SQL and returns a JSON chart specification that the frontend renders as an interactive Recharts visualization. Supports line, bar, area, and pie. |

### Write/Action Tools

| Tool | Purpose |
|------|---------|
| `save_query(name, sql, description)` | Persists a named SQL query to MongoDB for reuse. |
| `list_saved_queries()` | Lists all saved queries with name, description, and run count. |
| `run_saved_query(name)` | Fetches and executes a saved query by name from MongoDB. |

---

## SSE Streaming Protocol

The `/api/agent/chat` endpoint returns a Server-Sent Events stream with these event types:

| Event Type | When Sent | Payload |
|-----------|-----------|---------|
| `thread_id` | Start of stream | `{thread_id: "uuid"}` |
| `thinking` | Agent produces reasoning text before a tool call | `{content: "I'll check the daily metrics..."}` |
| `tool_call` | Agent decides to invoke a tool | `{id, name, input}` |
| `tool_result` | Tool execution completes | `{tool_call_id, name, output}` |
| `chart` | Tool result contains a chart spec | Full chart spec with data, type, title, axis config |
| `answer` | Agent produces final response (no more tool calls) | `{content: "Based on the data..."}` |
| `error` | Exception during processing | `{message: "error details"}` |
| `done` | Stream complete | `{thread_id: "uuid"}` |

---

## Frontend: Chat Interface

### Component: Agent.tsx

The chat interface displays:

1. **User messages** -- Right-aligned, blue background
2. **Reasoning steps** -- Collapsible section showing thinking text, tool calls (name + arguments), and tool results
3. **Inline charts** -- Rendered directly in the conversation using Recharts (LineChart, BarChart, AreaChart, PieChart)
4. **Final answer** -- The agent's synthesized response

### Thread Management

- Thread ID displayed in the header
- New Thread button resets all state
- Thread history persists server-side via MemorySaver (in-memory; resets on server restart)

---

## Example Interactions

**Simple query:**
User: "What's the total GMV?" -- Agent calls query_bigquery, returns the result.

**Multi-step investigation:**
User: "Why is the cancellation rate high?" -- Agent: (1) runs anomaly scan, (2) queries cancellation rate by date, (3) checks recent order statuses, (4) synthesizes findings.

**Chart generation:**
User: "Show me daily revenue for the last 90 days" -- Agent calls generate_chart with a line chart spec. Frontend renders an interactive chart inline.

**Memory recall:**
User: "What was that cancellation rate you found earlier?" -- Agent references previous findings from the same thread.

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Framework | LangGraph (custom StateGraph) | Stateful graph with checkpointing; create_react_agent is too opaque for debugging |
| State type | TypedDict with add_messages | Ensures message accumulation through graph cycles |
| LLM | Claude Sonnet 4 | Strong tool-use capabilities, fast enough for interactive use |
| Streaming | SSE (Server-Sent Events) | Simpler than WebSocket for unidirectional streaming |
| Memory | MemorySaver (in-memory) | Good for single-server; swap to Redis/Postgres for production |
| Tool safety | Read-only SQL enforcement | query_bigquery blocks INSERT/UPDATE/DELETE tokens |
| Chart rendering | JSON spec over image | Frontend renders interactive charts with hover tooltips |

---

## Troubleshooting

**"unexpected tool_use_id found in tool_result blocks"**
Cause: StateGraph initialized with `dict` instead of `AgentState`. Without the add_messages reducer, each node overwrites the message list. Fix: Use `StateGraph(AgentState)`.

**Agent returns blank / no response**
Cause: SSE parsing in the frontend not handling multi-line events. Fix: Line-by-line SSE parsing with proper buffering.

**"CancelledError" in server logs during reload**
Cause: Normal Uvicorn behavior on Windows during hot-reload. No fix needed. Verify "Application startup complete." appears after the error.

---

## Dependencies

| Package | Purpose |
|---------|---------|
| `langgraph` | StateGraph, ToolNode, MemorySaver |
| `langchain-anthropic` | Claude Sonnet 4 integration |
| `langchain-core` | Message types, tool decorator |
| `sse-starlette` | SSE response for FastAPI |

All listed in `pyproject.toml` under `[project.dependencies]`.

---

*Last updated: February 2026*
