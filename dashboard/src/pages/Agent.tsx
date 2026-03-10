import { useState, useRef, useEffect, useCallback, type FormEvent } from 'react'
import {
  LineChart, Line, BarChart, Bar, AreaChart, Area, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from 'recharts'

const API = import.meta.env.VITE_API_URL || 'http://localhost:8000'

const CHART_COLORS = [
  '#4C6FFF', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6',
  '#EC4899', '#06B6D4', '#F97316',
]

/* ────────────────────────────── types ────────────────────────────── */

interface ChartSpec {
  __chart__: true
  type: 'line' | 'bar' | 'area' | 'pie'
  title: string
  x_key: string
  y_keys: string[]
  x_label?: string
  y_label?: string
  data: Record<string, unknown>[]
}

interface Step {
  type: 'thinking' | 'tool_call' | 'tool_result'
  content?: string
  name?: string
  input?: Record<string, unknown>
  output?: string
}

interface Message {
  role: 'user' | 'assistant'
  content: string
  steps?: Step[]
  charts?: ChartSpec[]
}

/* ────────────────────────────── component ────────────────────────── */

export default function Agent() {
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const [streaming, setStreaming] = useState(false)
  const [liveSteps, setLiveSteps] = useState<Step[]>([])
  const [liveAnswer, setLiveAnswer] = useState('')
  const [liveCharts, setLiveCharts] = useState<ChartSpec[]>([])
  const [expandedIdx, setExpandedIdx] = useState<Record<number, boolean>>({})
  const [threadId, setThreadId] = useState<string | null>(null)
  const bottomRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLTextAreaElement>(null)

  const scroll = useCallback(
    () => bottomRef.current?.scrollIntoView({ behavior: 'smooth' }),
    [],
  )
  useEffect(scroll, [messages, liveSteps, liveAnswer, liveCharts, scroll])

  const newThread = () => {
    setMessages([])
    setThreadId(null)
    setLiveSteps([])
    setLiveAnswer('')
    setLiveCharts([])
    setExpandedIdx({})
  }

  /* ── send message ── */
  const send = async (e?: FormEvent) => {
    e?.preventDefault()
    const text = input.trim()
    if (!text || streaming) return

    setInput('')
    const userMsg: Message = { role: 'user', content: text }
    setMessages(prev => [...prev, userMsg])
    setStreaming(true)
    setLiveSteps([])
    setLiveAnswer('')
    setLiveCharts([])

    const steps: Step[] = []
    const charts: ChartSpec[] = []
    let answer = ''

    try {
      const payload = {
        messages: [{ role: 'user', content: text }],
        thread_id: threadId,
      }
      const res = await fetch(`${API}/api/agent/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })

      if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`)

      const reader = res.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ''
      let currentEvent = ''

      const processEvent = (eventType: string, data: string) => {
        if (!data) return
        let parsed: Record<string, unknown>
        try { parsed = JSON.parse(data) } catch { return }

        if (eventType === 'thread_id') {
          setThreadId(String(parsed.thread_id || ''))
        } else if (eventType === 'thinking') {
          steps.push({ type: 'thinking', content: String(parsed.content || '') })
          setLiveSteps([...steps])
        } else if (eventType === 'tool_call') {
          steps.push({
            type: 'tool_call',
            name: parsed.name as string,
            input: parsed.input as Record<string, unknown>,
          })
          setLiveSteps([...steps])
        } else if (eventType === 'tool_result') {
          steps.push({
            type: 'tool_result',
            name: parsed.name as string,
            output: parsed.output as string,
          })
          setLiveSteps([...steps])
        } else if (eventType === 'chart') {
          charts.push(parsed as unknown as ChartSpec)
          setLiveCharts([...charts])
        } else if (eventType === 'answer') {
          answer = String(parsed.content || '')
          setLiveAnswer(answer)
        } else if (eventType === 'error') {
          answer = `Error: ${parsed.message || 'Unknown error'}`
          setLiveAnswer(answer)
        }
      }

      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })

        const lines = buffer.split('\n')
        buffer = lines.pop() || ''

        for (const line of lines) {
          const trimmed = line.trim()
          if (trimmed.startsWith('event:')) {
            currentEvent = trimmed.substring(6).trim()
          } else if (trimmed.startsWith('data:')) {
            const data = trimmed.substring(5).trim()
            processEvent(currentEvent || 'message', data)
            currentEvent = ''
          }
        }
      }

      if (buffer.trim()) {
        const trimmed = buffer.trim()
        if (trimmed.startsWith('data:')) {
          processEvent(currentEvent || 'message', trimmed.substring(5).trim())
        }
      }

      setMessages(prev => [
        ...prev,
        {
          role: 'assistant',
          content: answer,
          steps: steps.length ? steps : undefined,
          charts: charts.length ? charts : undefined,
        },
      ])
      setLiveSteps([])
      setLiveAnswer('')
      setLiveCharts([])
    } catch (err) {
      if (answer || steps.length) {
        setMessages(prev => [
          ...prev,
          {
            role: 'assistant',
            content: answer || 'Connection interrupted during investigation.',
            steps: steps.length ? steps : undefined,
            charts: charts.length ? charts : undefined,
          },
        ])
        setLiveSteps([])
        setLiveAnswer('')
        setLiveCharts([])
      } else {
        setMessages(prev => [
          ...prev,
          { role: 'assistant', content: `Connection error: ${err}` },
        ])
      }
    } finally {
      setStreaming(false)
      inputRef.current?.focus()
    }
  }

  const toggleExpand = (idx: number) =>
    setExpandedIdx(prev => ({ ...prev, [idx]: !prev[idx] }))

  /* ── render ── */
  return (
    <div className="flex flex-col h-[calc(100vh-64px)]">
      {/* Header */}
      <div className="flex-shrink-0 pb-6 flex items-start justify-between">
        <div>
          <h1 className="text-[22px] font-extrabold text-gray-900 tracking-tight">
            AI Operations Agent
          </h1>
          <p className="text-[13px] text-gray-400 mt-1">
            Investigate metrics, diagnose issues, generate charts, run anomaly scans
            &mdash; with conversation memory.
          </p>
        </div>
        <div className="flex items-center gap-3 flex-shrink-0">
          {threadId && (
            <span className="text-[11px] text-gray-300 font-mono">
              {threadId.substring(0, 8)}
            </span>
          )}
          <button
            onClick={newThread}
            className="px-3 py-1.5 rounded-lg border border-gray-200 text-[12px] font-semibold text-gray-500 hover:bg-gray-50 hover:border-gray-300 transition-all"
          >
            New Thread
          </button>
        </div>
      </div>

      {/* Messages area */}
      <div className="flex-1 overflow-y-auto space-y-4 pb-4 min-h-0">
        {messages.length === 0 && !streaming && <EmptyState onExample={setInput} />}

        {messages.map((msg, i) => (
          <div key={i}>
            {msg.role === 'user' ? (
              <UserBubble content={msg.content} />
            ) : (
              <AssistantMessage
                content={msg.content}
                steps={msg.steps}
                charts={msg.charts}
                expanded={!!expandedIdx[i]}
                onToggle={() => toggleExpand(i)}
              />
            )}
          </div>
        ))}

        {/* Live streaming state */}
        {streaming && (
          <div className="space-y-3">
            {liveSteps.length > 0 && <LiveSteps steps={liveSteps} />}
            {liveCharts.length > 0 && liveCharts.map((c, i) => (
              <ChartCard key={i} spec={c} />
            ))}
            {liveAnswer && (
              <div className="card p-5">
                <div className="prose prose-sm max-w-none text-gray-800 whitespace-pre-wrap">
                  {liveAnswer}
                </div>
              </div>
            )}
            {!liveAnswer && (
              <div className="flex items-center gap-2 text-[13px] text-gray-400 pl-1">
                <span className="inline-block w-1.5 h-1.5 rounded-full bg-[#4C6FFF] animate-pulse" />
                {liveSteps.length ? 'Investigating...' : 'Thinking...'}
              </div>
            )}
          </div>
        )}

        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <form onSubmit={send} className="flex-shrink-0 pt-3 border-t border-gray-100">
        <div className="flex gap-3 items-end">
          <textarea
            ref={inputRef}
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={e => {
              if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send() }
            }}
            placeholder="Ask about revenue, experiments, pipeline health, or say 'chart GMV by month'..."
            rows={1}
            className="flex-1 resize-none rounded-xl border border-gray-200 bg-white px-4 py-3 text-[14px] text-gray-800 placeholder-gray-300 outline-none focus:border-[#4C6FFF] focus:ring-2 focus:ring-[#4C6FFF]/10 transition-all"
            disabled={streaming}
          />
          <button
            type="submit"
            disabled={streaming || !input.trim()}
            className="h-[46px] px-5 rounded-xl bg-[#4C6FFF] text-white text-[13px] font-semibold shadow-md shadow-blue-500/20 hover:bg-[#3B5EEE] disabled:opacity-40 disabled:cursor-not-allowed transition-all"
          >
            {streaming ? 'Working...' : 'Send'}
          </button>
        </div>
      </form>
    </div>
  )
}

/* ────────────────────────── sub-components ───────────────────────── */

function EmptyState({ onExample }: { onExample: (q: string) => void }) {
  const examples = [
    'Run an anomaly scan across all metrics',
    'Chart monthly GMV trend for the last 12 months',
    'Which product categories generate the most revenue? Show me a bar chart.',
    'Compare conversion rates across all A/B experiments',
    'Check pipeline health and data freshness',
    'What are our top 10 states by revenue? Visualize it.',
  ]
  return (
    <div className="flex flex-col items-center justify-center h-full text-center py-16">
      <div className="w-14 h-14 rounded-2xl bg-gradient-to-br from-[#4C6FFF] to-[#6C5CE7] flex items-center justify-center text-white font-extrabold text-lg shadow-lg shadow-blue-500/25 mb-5">
        AI
      </div>
      <h2 className="text-[18px] font-bold text-gray-800 mb-2">
        E-Commerce Operations Agent
      </h2>
      <p className="text-[13px] text-gray-400 max-w-md mb-2">
        Ask anything. Generate charts. Run anomaly scans. Save queries.
        Powered by a custom LangGraph agent with conversation memory.
      </p>
      <p className="text-[11px] text-gray-300 max-w-md mb-8">
        Charts render inline. Memory persists across messages in a thread.
      </p>
      <div className="grid gap-2 w-full max-w-lg">
        {examples.map((q, i) => (
          <button
            key={i}
            onClick={() => onExample(q)}
            className="text-left px-4 py-3 rounded-xl border border-gray-150 bg-white text-[13px] text-gray-600 hover:border-[#4C6FFF]/30 hover:bg-[#4C6FFF]/[0.03] transition-all"
          >
            {q}
          </button>
        ))}
      </div>
    </div>
  )
}

function UserBubble({ content }: { content: string }) {
  return (
    <div className="flex justify-end">
      <div className="max-w-[75%] bg-[#4C6FFF] text-white px-4 py-3 rounded-2xl rounded-br-md text-[14px] leading-relaxed shadow-md shadow-blue-500/15">
        {content}
      </div>
    </div>
  )
}

function AssistantMessage({
  content,
  steps,
  charts,
  expanded,
  onToggle,
}: {
  content: string
  steps?: Step[]
  charts?: ChartSpec[]
  expanded: boolean
  onToggle: () => void
}) {
  return (
    <div className="space-y-2 max-w-[90%]">
      {steps && steps.length > 0 && (
        <div className="card overflow-hidden">
          <button
            onClick={onToggle}
            className="w-full flex items-center justify-between px-4 py-2.5 text-[12px] font-semibold text-gray-400 bg-gray-50/80 hover:bg-gray-50 transition-colors"
          >
            <span>Reasoning &middot; {steps.length} steps</span>
            <svg
              className={`w-4 h-4 transition-transform ${expanded ? 'rotate-180' : ''}`}
              fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor"
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="m19.5 8.25-7.5 7.5-7.5-7.5" />
            </svg>
          </button>
          {expanded && (
            <div className="px-4 py-3 space-y-3 border-t border-gray-100">
              {steps.map((s, i) => <StepView key={i} step={s} />)}
            </div>
          )}
        </div>
      )}
      {charts && charts.map((c, i) => <ChartCard key={i} spec={c} />)}
      <div className="card p-5">
        <div className="text-[14px] text-gray-800 leading-relaxed whitespace-pre-wrap">
          {content}
        </div>
      </div>
    </div>
  )
}

function LiveSteps({ steps }: { steps: Step[] }) {
  return (
    <div className="card p-4 space-y-3 border-l-2 border-[#4C6FFF]/30">
      <span className="text-[11px] font-bold uppercase tracking-wider text-gray-300">
        Reasoning
      </span>
      {steps.map((s, i) => <StepView key={i} step={s} />)}
    </div>
  )
}

function StepView({ step }: { step: Step }) {
  const [showOutput, setShowOutput] = useState(false)

  if (step.type === 'thinking') {
    return (
      <div className="text-[13px] text-gray-500 italic leading-relaxed">
        {step.content}
      </div>
    )
  }

  if (step.type === 'tool_call') {
    const inputStr = step.input?.sql
      ? String(step.input.sql)
      : JSON.stringify(step.input, null, 2)
    return (
      <div className="rounded-lg bg-gray-50 border border-gray-100 overflow-hidden">
        <div className="px-3 py-2 flex items-center gap-2 text-[12px] font-semibold text-[#4C6FFF]">
          <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="m5.25 4.5 7.5 7.5-7.5 7.5m6-15 7.5 7.5-7.5 7.5" />
          </svg>
          {step.name}
        </div>
        <pre className="px-3 pb-2 text-[11px] text-gray-500 font-mono whitespace-pre-wrap break-all leading-relaxed max-h-32 overflow-y-auto">
          {inputStr}
        </pre>
      </div>
    )
  }

  if (step.type === 'tool_result') {
    const output = step.output || ''
    const preview = output.length > 200 ? output.substring(0, 200) + '...' : output
    return (
      <div className="rounded-lg bg-green-50/50 border border-green-100/60 overflow-hidden">
        <button
          onClick={() => setShowOutput(!showOutput)}
          className="w-full px-3 py-2 flex items-center justify-between text-[12px] font-semibold text-green-700/70"
        >
          <span>Result from {step.name}</span>
          <span className="text-[11px] text-green-600/40">
            {showOutput ? 'collapse' : 'expand'}
          </span>
        </button>
        <pre className="px-3 pb-2 text-[11px] text-gray-500 font-mono whitespace-pre-wrap break-all leading-relaxed max-h-48 overflow-y-auto">
          {showOutput ? output : preview}
        </pre>
      </div>
    )
  }

  return null
}


/* ────────────────────────── chart rendering ──────────────────────── */

function ChartCard({ spec }: { spec: ChartSpec }) {
  const { type, title, x_key, y_keys, x_label, y_label, data } = spec

  if (!data || data.length === 0) return null

  const formatValue = (v: unknown) => {
    if (typeof v === 'number') {
      if (Math.abs(v) >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`
      if (Math.abs(v) >= 1_000) return `${(v / 1_000).toFixed(1)}K`
      if (v % 1 !== 0) return v.toFixed(2)
      return v.toLocaleString()
    }
    return String(v ?? '')
  }

  const formatXTick = (v: unknown) => {
    const s = String(v ?? '')
    if (s.length > 12) return s.substring(0, 10) + '..'
    return s
  }

  return (
    <div className="card p-5">
      <h3 className="text-[14px] font-bold text-gray-800 mb-1">{title}</h3>
      {(x_label || y_label) && (
        <p className="text-[11px] text-gray-400 mb-3">
          {x_label && <>x: {x_label}</>}
          {x_label && y_label && ' · '}
          {y_label && <>y: {y_label}</>}
        </p>
      )}
      <div className="h-[280px] w-full">
        <ResponsiveContainer width="100%" height="100%">
          {type === 'pie' ? (
            <PieChart>
              <Pie
                data={data}
                dataKey={y_keys[0]}
                nameKey={x_key}
                cx="50%" cy="50%"
                outerRadius={100}
                innerRadius={50}
                label={({ name, percent }: { name: string; percent: number }) =>
                  `${name} ${(percent * 100).toFixed(0)}%`
                }
                labelLine={false}
              >
                {data.map((_, idx) => (
                  <Cell key={idx} fill={CHART_COLORS[idx % CHART_COLORS.length]} />
                ))}
              </Pie>
              <Tooltip formatter={formatValue} />
              <Legend />
            </PieChart>
          ) : type === 'bar' ? (
            <BarChart data={data} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
              <XAxis dataKey={x_key} tick={{ fontSize: 11 }} tickFormatter={formatXTick} />
              <YAxis tick={{ fontSize: 11 }} tickFormatter={formatValue} />
              <Tooltip formatter={formatValue} />
              {y_keys.length > 1 && <Legend />}
              {y_keys.map((k, i) => (
                <Bar key={k} dataKey={k} fill={CHART_COLORS[i % CHART_COLORS.length]} radius={[4, 4, 0, 0]} />
              ))}
            </BarChart>
          ) : type === 'area' ? (
            <AreaChart data={data} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
              <XAxis dataKey={x_key} tick={{ fontSize: 11 }} tickFormatter={formatXTick} />
              <YAxis tick={{ fontSize: 11 }} tickFormatter={formatValue} />
              <Tooltip formatter={formatValue} />
              {y_keys.length > 1 && <Legend />}
              {y_keys.map((k, i) => (
                <Area
                  key={k} type="monotone" dataKey={k}
                  stroke={CHART_COLORS[i % CHART_COLORS.length]}
                  fill={CHART_COLORS[i % CHART_COLORS.length]}
                  fillOpacity={0.15}
                />
              ))}
            </AreaChart>
          ) : (
            <LineChart data={data} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
              <XAxis dataKey={x_key} tick={{ fontSize: 11 }} tickFormatter={formatXTick} />
              <YAxis tick={{ fontSize: 11 }} tickFormatter={formatValue} />
              <Tooltip formatter={formatValue} />
              {y_keys.length > 1 && <Legend />}
              {y_keys.map((k, i) => (
                <Line
                  key={k} type="monotone" dataKey={k}
                  stroke={CHART_COLORS[i % CHART_COLORS.length]}
                  strokeWidth={2} dot={{ r: 2 }}
                />
              ))}
            </LineChart>
          )}
        </ResponsiveContainer>
      </div>
    </div>
  )
}
