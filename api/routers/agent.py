"""AI Agent endpoint — streams reasoning steps via Server-Sent Events.

Supports:
  - Thread-based conversations with persistent memory
  - Chart generation (returns chart specs inline)
  - Anomaly detection results
  - Saved query management
"""
from __future__ import annotations

import json
import logging
import uuid
from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from api.agent.graph import build_agent_with_prompt

log = logging.getLogger(__name__)
router = APIRouter()


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    messages: list[ChatMessage]
    thread_id: Optional[str] = None


class ThreadResponse(BaseModel):
    threads: list[dict]


def _sse(event: str, data: dict[str, Any]) -> dict:
    return {"event": event, "data": json.dumps(data, default=str)}


def _truncate(text: str, limit: int = 1500) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n... (truncated, {len(text)} chars total)"


def _extract_text(content) -> str:
    """Extract plain text from Claude's content (string or list of blocks)."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and block.get("type") == "text":
                parts.append(block["text"])
        return "\n".join(parts)
    return str(content)


def _try_parse_chart(text: str) -> dict | None:
    """Check if a tool result contains a chart spec."""
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict) and parsed.get("__chart__"):
            return parsed
    except (json.JSONDecodeError, TypeError):
        pass
    return None


@router.post("/chat")
async def agent_chat(req: ChatRequest):
    """Stream agent reasoning as SSE events.

    Event types sent to the client:
      thinking    -- agent's reasoning text before a tool call
      tool_call   -- tool name + arguments being invoked
      tool_result -- truncated output from the tool
      chart       -- chart specification for frontend rendering
      answer      -- final response text (no more tool calls)
      error       -- something went wrong
      done        -- stream complete (includes thread_id for persistence)
    """
    if not req.messages:
        raise HTTPException(400, "messages list cannot be empty")

    thread_id = req.thread_id or str(uuid.uuid4())

    agent, system_prompt = build_agent_with_prompt()

    lc_messages: list = []

    config = {"configurable": {"thread_id": thread_id}}

    existing_state = None
    try:
        existing_state = agent.get_state(config)
    except Exception:
        pass

    has_history = (
        existing_state is not None
        and existing_state.values
        and existing_state.values.get("messages")
    )

    if not has_history:
        lc_messages.append(SystemMessage(content=system_prompt))

    for m in req.messages:
        if m.role == "user":
            lc_messages.append(HumanMessage(content=m.content))
        elif m.role == "assistant":
            if not has_history:
                lc_messages.append(AIMessage(content=m.content))

    if has_history:
        input_messages = []
        for m in req.messages:
            if m.role == "user":
                input_messages.append(HumanMessage(content=m.content))
        lc_messages = input_messages

    async def event_stream():
        try:
            yield _sse("thread_id", {"thread_id": thread_id})

            async for chunk in agent.astream(
                {"messages": lc_messages},
                config=config,
                stream_mode="updates",
            ):
                for node_name, node_output in chunk.items():
                    messages = node_output.get("messages", [])
                    for msg in messages:
                        if isinstance(msg, AIMessage):
                            text = _extract_text(msg.content)
                            if msg.tool_calls:
                                if text:
                                    yield _sse("thinking", {"content": text})
                                for tc in msg.tool_calls:
                                    yield _sse("tool_call", {
                                        "id": tc["id"],
                                        "name": tc["name"],
                                        "input": tc["args"],
                                    })
                            else:
                                if text:
                                    yield _sse("answer", {"content": text})

                        elif isinstance(msg, ToolMessage):
                            chart = _try_parse_chart(msg.content)
                            if chart:
                                yield _sse("chart", chart)
                                yield _sse("tool_result", {
                                    "tool_call_id": msg.tool_call_id,
                                    "name": msg.name,
                                    "output": f"Chart generated: {chart.get('title', '')} ({len(chart.get('data', []))} data points)",
                                })
                            else:
                                yield _sse("tool_result", {
                                    "tool_call_id": msg.tool_call_id,
                                    "name": msg.name,
                                    "output": _truncate(msg.content),
                                })
        except Exception as e:
            log.exception("Agent error")
            yield _sse("error", {"message": str(e)})

        yield _sse("done", {"thread_id": thread_id})

    return EventSourceResponse(event_stream())
