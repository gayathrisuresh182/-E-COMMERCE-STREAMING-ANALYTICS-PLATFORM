import { useEffect, useState } from "react";
import {
  Area, AreaChart, CartesianGrid, Line, LineChart, ReferenceLine,
  ResponsiveContainer, Tooltip, XAxis, YAxis,
} from "recharts";
import {
  api, type Alert, type DataFreshness, type HealthScore, type KafkaConsumer,
  type LagHistoryPoint, type MongoCollection, type SlaDay, type ThroughputPoint,
} from "../api";

const TIP = { background: "#fff", border: "1px solid #E2E8F0", borderRadius: 14, fontSize: 12, boxShadow: "0 10px 30px rgba(0,0,0,0.1)" };
const timeLabel = (iso: string) => new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });

function safeMinutesAgo(iso: string): number | null {
  if (!iso) return null;
  const t = new Date(iso).getTime();
  if (isNaN(t)) return null;
  return Math.round((Date.now() - t) / 60_000);
}

function agoLabel(iso: string): string {
  const m = safeMinutesAgo(iso);
  if (m === null) return "—";
  if (m < 0) return "just now";
  if (m >= 1440) return `${Math.round(m / 1440)}d ago`;
  if (m >= 60) return `${Math.round(m / 60)}h ago`;
  return `${m}m ago`;
}

function Section({ title, sub, children, className = "" }: { title: string; sub?: string; children: React.ReactNode; className?: string }) {
  return (
    <div className={`card p-6 ${className}`}>
      <div className="mb-5"><h3 className="section-title">{title}</h3>{sub && <p className="section-sub">{sub}</p>}</div>
      {children}
    </div>
  );
}

function Loading() {
  return (
    <div className="space-y-6 animate-pulse">
      <div className="h-9 w-64 rounded-lg bg-gray-200" />
      <div className="grid grid-cols-2 md:grid-cols-4 gap-5">{Array.from({ length: 4 }).map((_, i) => <div key={i} className="h-28 rounded-2xl bg-gray-100" />)}</div>
      <div className="h-72 rounded-2xl bg-gray-100" />
    </div>
  );
}

function EmptyChart({ text }: { text: string }) {
  return (
    <div className="h-64 flex flex-col items-center justify-center text-center gap-3 rounded-xl bg-gray-50 border-2 border-dashed border-gray-200">
      <svg className="w-10 h-10 text-gray-300" fill="none" viewBox="0 0 24 24" strokeWidth={1} stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 0 1 3 19.875v-6.75ZM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 0 1-1.125-1.125V8.625ZM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 0 1-1.125-1.125V4.125Z" /></svg>
      <p className="text-[13px] text-gray-400 font-medium max-w-[220px]">{text}</p>
    </div>
  );
}

function HealthGauge({ score, status }: { score: number; status: string }) {
  const color = status === "excellent" ? "#00B894" : status === "warning" ? "#FDCB6E" : "#E17055";
  const c = 2 * Math.PI * 52;
  return (
    <div className="flex flex-col items-center">
      <svg width="150" height="150" viewBox="0 0 150 150">
        <circle cx="75" cy="75" r="52" fill="none" stroke="#EDF2F7" strokeWidth="10" />
        <circle cx="75" cy="75" r="52" fill="none" stroke={color} strokeWidth="10" strokeLinecap="round" strokeDasharray={c} strokeDashoffset={c * (1 - score / 100)} transform="rotate(-90 75 75)" className="transition-all duration-1000" />
        <text x="75" y="68" textAnchor="middle" fill="#1A202C" fontSize="34" fontWeight="800">{score}</text>
        <text x="75" y="90" textAnchor="middle" fill="#94A3B8" fontSize="12" fontWeight="600" textTransform="uppercase">{status}</text>
      </svg>
    </div>
  );
}

function StatusDot({ status }: { status: string }) {
  const c = status === "healthy" || status === "fresh" ? "bg-emerald-400" : status === "warning" || status === "stale" ? "bg-amber-400" : "bg-red-400";
  return <span className={`inline-block w-2.5 h-2.5 rounded-full ${c}`} />;
}

function SeverityBadge({ severity }: { severity: string }) {
  const c = severity === "critical" ? "bg-red-50 text-red-600 border-red-200" : "bg-amber-50 text-amber-700 border-amber-200";
  return <span className={`px-2.5 py-1 rounded-lg text-[10px] font-bold border ${c}`}>{severity}</span>;
}

function AlertStatusBadge({ status }: { status: string }) {
  return status === "open"
    ? <span className="px-2.5 py-1 rounded-lg text-[10px] font-bold bg-red-50 text-red-600 border border-red-200">Open</span>
    : <span className="px-2.5 py-1 rounded-lg text-[10px] font-bold bg-gray-100 text-gray-400 border border-gray-200">Resolved</span>;
}

function FreshnessTimeline({ data }: { data: DataFreshness[] }) {
  return (
    <div className="space-y-4">
      {data.map((d) => {
        const ago = safeMinutesAgo(d.last_updated);
        const label = agoLabel(d.last_updated);
        const slaMin = d.sla_hours * 60;
        const pct = ago !== null ? Math.min((ago / slaMin) * 100, 100) : 0;
        const bar = d.status === "fresh" ? "bg-emerald-400" : "bg-amber-400";
        return (
          <div key={d.dataset} className="flex items-center gap-3">
            <StatusDot status={d.status} />
            <div className="flex-1 min-w-0">
              <div className="flex justify-between text-[13px]"><span className="text-gray-600 font-semibold truncate">{d.dataset}</span><span className="text-gray-400 font-medium shrink-0 ml-2">{label}</span></div>
              <div className="flex items-center gap-3 mt-1.5">
                <div className="flex-1 h-2 rounded-full bg-gray-100 overflow-hidden"><div className={`h-full rounded-full ${bar}`} style={{ width: `${pct}%` }} /></div>
                <span className="text-[11px] text-gray-400 font-semibold shrink-0">{d.update_frequency}</span>
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

function SlaCalendar({ data }: { data: SlaDay[] }) {
  const met = data.filter((d) => d.sla_met).length;
  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <span className="text-[13px] text-gray-500 font-medium">{met}/{data.length} days met SLA</span>
        <span className="text-[14px] font-bold text-emerald-600">{data.length ? ((met / data.length) * 100).toFixed(1) : 0}%</span>
      </div>
      <div className="flex flex-wrap gap-1.5">
        {data.map((d) => <div key={d.date} className={`w-[20px] h-[20px] rounded-[5px] ${d.sla_met ? "bg-emerald-400" : "bg-red-300"} transition-all hover:ring-2 hover:ring-[#4C6FFF]/30`} title={`${d.date}: ${d.duration_min}min (SLA: ${d.sla_target_min}min)`} />)}
      </div>
      <div className="flex items-center gap-4 mt-3 text-[11px] text-gray-400 font-semibold">
        <span className="flex items-center gap-1.5"><span className="w-3 h-3 rounded-[4px] bg-emerald-400" /> Met</span>
        <span className="flex items-center gap-1.5"><span className="w-3 h-3 rounded-[4px] bg-red-300" /> Missed</span>
      </div>
    </div>
  );
}

export default function DataQuality() {
  const [health, setHealth] = useState<HealthScore | null>(null);
  const [consumers, setConsumers] = useState<KafkaConsumer[]>([]);
  const [collections, setCollections] = useState<MongoCollection[]>([]);
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [lagHistory, setLagHistory] = useState<LagHistoryPoint[]>([]);
  const [throughput, setThroughput] = useState<ThroughputPoint[]>([]);
  const [sla, setSla] = useState<SlaDay[]>([]);
  const [freshness, setFreshness] = useState<DataFreshness[]>([]);

  useEffect(() => {
    Promise.all([
      api.monitoring.healthScore().then(setHealth), api.monitoring.consumers().then(setConsumers),
      api.monitoring.collections().then(setCollections), api.monitoring.alerts().then(setAlerts),
      api.monitoring.lagHistory().then(setLagHistory), api.monitoring.throughput().then(setThroughput),
      api.monitoring.slaCompliance().then(setSla), api.monitoring.dataFreshness().then(setFreshness),
    ]);
  }, []);

  if (!health) return <Loading />;

  const lagKeys = lagHistory.length > 0 ? Object.keys(lagHistory[0]).filter((k) => k !== "timestamp") : [];
  const lagColors = ["#4C6FFF", "#00B894", "#E17055", "#FDCB6E"];

  return (
    <div className="space-y-7">
      <div className="flex items-center justify-between anim-fade">
        <div>
          <h2 className="text-[22px] font-extrabold text-gray-900 tracking-tight">Data Quality & Pipeline Health</h2>
          <p className="text-[13px] text-gray-400 mt-1 font-medium">Operational monitoring — Dagster, Kafka, MongoDB, BigQuery</p>
        </div>
        <div className="text-[11px] text-gray-400 text-right font-medium">Auto-refreshes every 5 min<br /><span className="text-gray-500 font-semibold">Last update: just now</span></div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-5 gap-5">
        <div className="md:col-span-1 card p-6 flex items-center justify-center"><HealthGauge score={health.score} status={health.status} /></div>
        <div className="md:col-span-4 grid grid-cols-2 md:grid-cols-4 gap-4">
          {[
            { l: "Healthy Consumers", v: `${health.healthy_consumers}/${health.total_consumers}`, c: "text-emerald-600", ac: "#00B894" },
            { l: "Open Alerts", v: health.open_alerts.toString(), c: health.open_alerts > 0 ? "text-red-500" : "text-emerald-600", ac: health.open_alerts > 0 ? "#E17055" : "#00B894" },
            { l: "MongoDB Collections", v: collections.length.toString(), c: "text-[#4C6FFF]", ac: "#4C6FFF" },
            { l: "Total Documents", v: collections.reduce((s, c) => s + c.document_count, 0).toLocaleString(), c: "text-gray-800", ac: "#636E72" },
          ].map((k) => (
            <div key={k.l} className="kpi-card p-5" style={{ "--accent": k.ac } as React.CSSProperties}>
              <div className="kpi-label">{k.l}</div>
              <div className={`kpi-value mt-2 ${k.c}`}>{k.v}</div>
            </div>
          ))}
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Section title="Kafka Consumer Groups" sub="Lag and health status per consumer">
          {consumers.length === 0 ? (
            <div className="py-10 text-center text-[13px] text-gray-400 font-medium">No active consumers — start Kafka consumers to see lag data</div>
          ) : (
            <div className="space-y-3">{consumers.map((c) => (
              <div key={c.consumer_group} className="flex items-center gap-4 p-4 rounded-xl bg-gray-50 border border-gray-100">
                <StatusDot status={c.status} />
                <div className="flex-1 min-w-0">
                  <div className="text-[13px] text-gray-700 font-bold">{c.consumer_group}</div>
                  <div className="text-[11px] text-gray-400 font-medium mt-0.5">{c.topics}</div>
                </div>
                <div className="text-right">
                  <div className={`text-[16px] font-extrabold ${c.total_lag > 2000 ? "text-red-500" : c.total_lag > 500 ? "text-amber-500" : "text-emerald-600"}`}>{c.total_lag.toLocaleString()}</div>
                  <div className="text-[10px] text-gray-400 font-semibold uppercase tracking-wide">lag msgs</div>
                </div>
              </div>
            ))}</div>
          )}
        </Section>

        <Section title="MongoDB Collections" sub="Document counts and data freshness">
          <div className="space-y-3">{collections.map((c) => {
            const ago = agoLabel(c.newest_timestamp);
            const m = safeMinutesAgo(c.newest_timestamp);
            const fresh = m !== null && m < 1440;
            return (
              <div key={c.collection} className="flex items-center gap-4 p-4 rounded-xl bg-gray-50 border border-gray-100">
                <StatusDot status={fresh ? "healthy" : "warning"} />
                <div className="flex-1 min-w-0">
                  <div className="text-[13px] text-gray-700 font-bold">{c.label}</div>
                  <div className="text-[11px] text-gray-400 font-mono mt-0.5">{c.collection}</div>
                </div>
                <div className="text-right">
                  <div className="text-[16px] font-extrabold text-gray-800">{c.document_count.toLocaleString()}</div>
                  <div className={`text-[11px] font-semibold ${fresh ? "text-emerald-500" : "text-gray-400"}`}>{ago}</div>
                </div>
              </div>
            );
          })}</div>
        </Section>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Section title="Consumer Lag History" sub="24-hour lag trends — spike indicates bottleneck">
          {lagHistory.length === 0 ? <EmptyChart text="No lag data yet — metrics appear after consumer sync runs" /> : (
            <div className="h-72">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={lagHistory}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#EDF2F7" />
                  <XAxis dataKey="timestamp" tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} tickFormatter={timeLabel} interval={15} />
                  <YAxis tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} />
                  <Tooltip contentStyle={TIP} labelFormatter={timeLabel} />
                  <ReferenceLine y={5000} stroke="#E17055" strokeDasharray="6 3" label={{ value: "Alert", fill: "#E17055", fontSize: 11, fontWeight: 700, position: "right" }} />
                  {lagKeys.map((key, i) => <Line key={key} type="monotone" dataKey={key} stroke={lagColors[i % lagColors.length]} strokeWidth={2} dot={false} />)}
                </LineChart>
              </ResponsiveContainer>
            </div>
          )}
        </Section>

        <Section title="Pipeline Throughput" sub="Messages/sec flowing through Kafka topics">
          {throughput.length === 0 ? <EmptyChart text="No throughput data — pipeline metrics appear once streaming is active" /> : (
            <div className="h-72">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={throughput}>
                  <defs>
                    <linearGradient id="tpC" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#4C6FFF" stopOpacity={0.12} /><stop offset="100%" stopColor="#4C6FFF" stopOpacity={0} /></linearGradient>
                    <linearGradient id="tpO" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#00B894" stopOpacity={0.12} /><stop offset="100%" stopColor="#00B894" stopOpacity={0} /></linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="#EDF2F7" />
                  <XAxis dataKey="timestamp" tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} tickFormatter={timeLabel} interval={15} />
                  <YAxis tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} />
                  <Tooltip contentStyle={TIP} labelFormatter={timeLabel} />
                  <Area type="monotone" dataKey="clickstream" stroke="#4C6FFF" fill="url(#tpC)" strokeWidth={2} dot={false} name="Clickstream" />
                  <Area type="monotone" dataKey="orders_stream" stroke="#00B894" fill="url(#tpO)" strokeWidth={2} dot={false} name="Orders" />
                  <Line type="monotone" dataKey="deliveries_stream" stroke="#FDCB6E" strokeWidth={1.5} dot={false} name="Deliveries" />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          )}
        </Section>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Section title="SLA Compliance — Last 30 Days" sub="Daily batch pipeline — target: 24 hours"><SlaCalendar data={sla} /></Section>
        <Section title="Data Freshness" sub="Per-dataset update status relative to SLA"><FreshnessTimeline data={freshness} /></Section>
      </div>

      <Section title="Recent Alerts" sub="Pipeline incidents and data quality issues">
        {alerts.length === 0 ? (
          <div className="py-10 text-center text-[13px] text-gray-400 font-medium">No alerts — pipeline operating normally</div>
        ) : (
          <div className="space-y-3">{alerts.map((a, i) => (
            <div key={i} className={`flex items-start gap-4 p-4 rounded-xl border-2 ${a.status === "open" ? "bg-red-50/40 border-red-200" : "bg-gray-50 border-gray-100"}`}>
              <div className="shrink-0 mt-0.5"><SeverityBadge severity={a.severity} /></div>
              <div className="flex-1 min-w-0">
                <div className="text-[13px] text-gray-700 font-semibold">{a.message}</div>
                <div className="flex items-center gap-2 mt-1.5">
                  <span className="text-[11px] text-gray-400 font-medium">{a.type}</span>
                  <span className="text-gray-300">·</span>
                  <span className="text-[11px] text-gray-400 font-medium">{new Date(a.timestamp).toLocaleString()}</span>
                </div>
              </div>
              <AlertStatusBadge status={a.status} />
            </div>
          ))}</div>
        )}
      </Section>
    </div>
  );
}
