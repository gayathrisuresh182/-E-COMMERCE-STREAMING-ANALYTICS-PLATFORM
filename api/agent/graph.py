"""
Custom LangGraph agent with persistent memory and multi-node architecture.

Graph structure:
  START → agent (ReAct with tools) → END

Features beyond create_react_agent:
  - MemorySaver checkpointer for cross-session conversation memory
  - Thread-based conversations (each thread retains full history)
  - Custom state with metadata tracking (tool call count, intent)
  - Singleton agent instance (built once, reused across requests)
"""
from __future__ import annotations

import os
from typing import Annotated

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import AnyMessage
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode
from typing_extensions import TypedDict

from api.agent.prompts import SYSTEM_PROMPT
from api.agent.tools import get_all_tools

# ── State ────────────────────────────────────────────────────────────────

class AgentState(TypedDict):
    """State with add_messages reducer so messages accumulate through the graph."""
    messages: Annotated[list[AnyMessage], add_messages]
    tool_call_count: int


# ── Nodes ────────────────────────────────────────────────────────────────

def _build_model():
    return ChatAnthropic(
        model="claude-sonnet-4-20250514",
        temperature=0,
        max_tokens=4096,
        api_key=os.environ.get("ANTHROPIC_API_KEY"),
    )


_tools = get_all_tools()
_model = None


def _get_model():
    global _model
    if _model is None:
        _model = _build_model().bind_tools(_tools)
    return _model


def agent_node(state: dict) -> dict:
    """Main reasoning node: calls the LLM with tools bound."""
    messages = state.get("messages", [])
    model = _get_model()
    response = model.invoke(messages)

    tool_count = state.get("tool_call_count", 0)
    if hasattr(response, "tool_calls") and response.tool_calls:
        tool_count += len(response.tool_calls)

    return {"messages": [response], "tool_call_count": tool_count}


def should_continue(state: dict) -> str:
    """Route: if the last message has tool calls, go to tools; else finish."""
    messages = state.get("messages", [])
    if not messages:
        return END
    last = messages[-1]
    if hasattr(last, "tool_calls") and last.tool_calls:
        return "tools"
    return END


# ── Graph Construction ───────────────────────────────────────────────────

_agent = None
_checkpointer = None


def get_checkpointer() -> MemorySaver:
    global _checkpointer
    if _checkpointer is None:
        _checkpointer = MemorySaver()
    return _checkpointer


def build_agent():
    """Build (or return cached) the compiled LangGraph agent with memory."""
    global _agent

    if _agent is not None:
        return _agent

    graph = StateGraph(AgentState)

    graph.add_node("agent", agent_node)
    graph.add_node("tools", ToolNode(_tools))

    graph.add_edge(START, "agent")
    graph.add_conditional_edges("agent", should_continue, {"tools": "tools", END: END})
    graph.add_edge("tools", "agent")

    checkpointer = get_checkpointer()

    _agent = graph.compile(checkpointer=checkpointer)
    return _agent


def build_agent_with_prompt():
    """Return the agent and the system prompt (used by the router to inject
    the system message on first request for a thread)."""
    return build_agent(), SYSTEM_PROMPT
