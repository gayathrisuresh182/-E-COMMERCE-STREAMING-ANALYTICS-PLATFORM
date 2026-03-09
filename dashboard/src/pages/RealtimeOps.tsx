import { useEffect, useState } from "react";
import {
  Area, AreaChart, Bar, BarChart, CartesianGrid, Cell, Line, LineChart,
  Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from "recharts";
import {
  api, type CancellationPoint, type HeatmapCell, type RealtimeKpis,
  type RecentOrder, type StateData, type StatusData, type TimeseriesPoint,
} from "../api";

const fmt = (n: number) =>
  n >= 1_000_000 ? `R$${(n / 1_000_000).toFixed(2)}M` : n >= 1_000 ? `R$${(n / 1000).toFixed(1)}K` : `R$${n.toFixed(2)}`;
const timeLabel = (iso: string) => new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
const TIP = { background: "#fff", border: "1px solid #E2E8F0", borderRadius: 14, fontSize: 12, boxShadow: "0 10px 30px rgba(0,0,0,0.1)" };

function Spark({ data, color = "#4C6FFF" }: { data: number[]; color?: string }) {
  const pts = data.map((v, i) => ({ v, i }));
  const id = `sp-${color.replace("#", "")}`;
  return (
    <div className="h-9 w-28">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={pts} margin={{ top: 2, right: 0, bottom: 0, left: 0 }}>
          <defs><linearGradient id={id} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity={0.2} /><stop offset="100%" stopColor={color} stopOpacity={0} />
          </linearGradient></defs>
          <Area type="monotone" dataKey="v" stroke={color} strokeWidth={2} fill={`url(#${id})`} dot={false} />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

function Section({ title, sub, children, className = "" }: { title: string; sub?: string; children: React.ReactNode; className?: string }) {
  return (
    <div className={`card p-6 ${className}`}>
      <div className="mb-5">
        <h3 className="section-title">{title}</h3>
        {sub && <p className="section-sub">{sub}</p>}
      </div>
      {children}
    </div>
  );
}

function Loading() {
  return (
    <div className="space-y-6 animate-pulse">
      <div className="h-9 w-64 rounded-lg bg-gray-200" />
      <div className="grid grid-cols-3 xl:grid-cols-6 gap-5">{Array.from({ length: 6 }).map((_, i) => <div key={i} className="h-28 rounded-2xl bg-gray-100" />)}</div>
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-5"><div className="h-80 rounded-2xl bg-gray-100" /><div className="h-80 rounded-2xl bg-gray-100" /></div>
    </div>
  );
}

function Heatmap({ data }: { data: HeatmapCell[] }) {
  const days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
  const maxVal = Math.max(...data.map((d) => d.orders), 1);
  const color = (v: number) => {
    const r = v / maxVal;
    if (r > 0.8) return "bg-[#4C6FFF]";
    if (r > 0.6) return "bg-[#4C6FFF]/70";
    if (r > 0.4) return "bg-[#4C6FFF]/45";
    if (r > 0.2) return "bg-[#4C6FFF]/20";
    return "bg-gray-100";
  };
  return (
    <div className="overflow-x-auto">
      <div className="flex gap-[4px]">
        <div className="flex flex-col gap-[4px] mt-6 mr-2">
          {days.map((d) => <div key={d} className="h-[22px] flex items-center text-[11px] text-gray-400 font-medium">{d}</div>)}
        </div>
        <div>
          <div className="flex gap-[4px] mb-[4px]">
            {Array.from({ length: 24 }).map((_, h) => <div key={h} className="w-[22px] text-center text-[10px] text-gray-400 font-medium">{h}</div>)}
          </div>
          {days.map((day) => (
            <div key={day} className="flex gap-[4px]">
              {Array.from({ length: 24 }).map((_, hour) => {
                const cell = data.find((d) => d.day === day && d.hour === hour);
                return <div key={hour} className={`w-[22px] h-[22px] rounded-[5px] ${color(cell?.orders ?? 0)} transition-all hover:ring-2 hover:ring-[#4C6FFF]/40`} title={`${day} ${hour}:00 — ${cell?.orders ?? 0} orders`} />;
              })}
            </div>
          ))}
        </div>
      </div>
      <div className="flex items-center gap-2 mt-3 ml-10">
        <span className="text-[10px] text-gray-400 font-medium">Less</span>
        {["bg-gray-100", "bg-[#4C6FFF]/20", "bg-[#4C6FFF]/45", "bg-[#4C6FFF]/70", "bg-[#4C6FFF]"].map((c) => <div key={c} className={`w-[14px] h-[14px] rounded-[4px] ${c}`} />)}
        <span className="text-[10px] text-gray-400 font-medium">More</span>
      </div>
    </div>
  );
}

function StatusBadge({ status }: { status: string }) {
  const m: Record<string, string> = { order_placed: "bg-blue-50 text-blue-700 font-semibold", order_shipped: "bg-green-50 text-green-700 font-semibold", order_canceled: "bg-red-50 text-red-600 font-semibold" };
  return <span className={`px-2.5 py-1 rounded-lg text-[10px] uppercase ${m[status] ?? "bg-gray-100 text-gray-500"}`}>{status.replace("order_", "")}</span>;
}

export default function RealtimeOps() {
  const [kpis, setKpis] = useState<RealtimeKpis | null>(null);
  const [ts, setTs] = useState<TimeseriesPoint[]>([]);
  const [states, setStates] = useState<StateData[]>([]);
  const [statuses, setStatuses] = useState<StatusData[]>([]);
  const [orders, setOrders] = useState<RecentOrder[]>([]);
  const [heatmap, setHeatmap] = useState<HeatmapCell[]>([]);
  const [cancelTrend, setCancelTrend] = useState<CancellationPoint[]>([]);

  useEffect(() => {
    Promise.all([
      api.realtime.kpis().then(setKpis), api.realtime.timeseries().then(setTs),
      api.realtime.ordersByState().then(setStates), api.realtime.ordersByStatus().then(setStatuses),
      api.realtime.recentOrders().then(setOrders), api.realtime.heatmap().then(setHeatmap),
      api.realtime.cancellationTrend().then(setCancelTrend),
    ]);
  }, []);

  if (!kpis) return <Loading />;

  const statusTotal = statuses.reduce((s, d) => s + d.count, 0) || 1;
  const tsSlice = ts.slice(-72);
  const STATUS_COLORS: Record<string, string> = { order_placed: "#4C6FFF", order_shipped: "#00B894", order_canceled: "#E17055" };

  return (
    <div className="space-y-7">
      <div className="flex items-center justify-between anim-fade">
        <div>
          <h2 className="text-[22px] font-extrabold text-gray-900 tracking-tight">Real-Time Operations</h2>
          <p className="text-[13px] text-gray-400 mt-1 font-medium">Live streaming data — auto-refreshes every 30s</p>
        </div>
        <div className="flex items-center gap-2 badge-live px-4 py-1.5 rounded-full text-[11px]">
          <span className="relative flex h-2 w-2"><span className="animate-ping absolute h-full w-full rounded-full bg-green-400 opacity-50" /><span className="relative h-2 w-2 rounded-full bg-green-500" /></span>
          Live
        </div>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-5">
        {[
          { l: "GMV Today", v: fmt(kpis.total_gmv), d: kpis.gmv_vs_yesterday, sp: kpis.sparklines.gmv, c: "#4C6FFF", ac: "#4C6FFF" },
          { l: "Orders Today", v: kpis.total_orders.toLocaleString(), d: kpis.orders_vs_yesterday, sp: kpis.sparklines.orders, c: "#00B894", ac: "#00B894" },
          { l: "Avg Order Value", v: `R$${kpis.avg_order_value.toFixed(2)}`, sp: kpis.sparklines.aov, c: "#6C5CE7", ac: "#6C5CE7" },
          { l: "Orders / min", v: kpis.orders_per_minute.toFixed(1), ac: "#FDCB6E" },
          { l: "Active Sessions", v: kpis.active_sessions.toLocaleString(), ac: "#00CEC9" },
          { l: "Cancel Rate", v: `${kpis.cancel_rate.toFixed(1)}%`, sp: kpis.sparklines.cancel_rate, c: "#E17055", ac: "#E17055" },
        ].map((k, i) => (
          <div key={k.l} className={`kpi-card p-5 anim-fade anim-d${i + 1}`} style={{ "--accent": k.ac } as React.CSSProperties}>
            <span className="kpi-label">{k.l}</span>
            <div className="flex items-end justify-between mt-2">
              <span className="kpi-value">{k.v}</span>
              {k.sp && <Spark data={k.sp} color={k.c} />}
            </div>
            {k.d !== undefined && (
              <div className="flex items-center gap-1.5 mt-2">
                <span className={`text-[12px] font-bold ${k.d >= 0 ? "text-emerald-600" : "text-red-500"}`}>{k.d >= 0 ? "+" : ""}{k.d.toFixed(1)}%</span>
                <span className="text-[11px] text-gray-400">vs yesterday</span>
              </div>
            )}
          </div>
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <Section title="GMV — Today vs Yesterday" sub="5-minute intervals" className="lg:col-span-2">
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={tsSlice}>
                <defs>
                  <linearGradient id="gFill" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#4C6FFF" stopOpacity={0.12} /><stop offset="100%" stopColor="#4C6FFF" stopOpacity={0} /></linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#EDF2F7" />
                <XAxis dataKey="timestamp" tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} tickFormatter={timeLabel} interval={11} />
                <YAxis tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} tickFormatter={fmt} />
                <Tooltip contentStyle={TIP} formatter={(v: number, name: string) => [fmt(v), name === "gmv" ? "Today" : "Yesterday"]} labelFormatter={timeLabel} />
                <Area type="monotone" dataKey="gmv" stroke="#4C6FFF" strokeWidth={2.5} fill="url(#gFill)" dot={false} />
                <Area type="monotone" dataKey="gmv_yesterday" stroke="#CBD5E0" strokeWidth={1.5} strokeDasharray="5 4" fill="none" dot={false} />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </Section>

        <Section title="Order Status" sub="Distribution today">
          <div className="h-52 flex justify-center">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart><Pie data={statuses} dataKey="count" nameKey="status" cx="50%" cy="50%" innerRadius={50} outerRadius={80} paddingAngle={3} strokeWidth={0}>{statuses.map((s) => <Cell key={s.status} fill={STATUS_COLORS[s.status] || "#CBD5E0"} />)}</Pie><Tooltip contentStyle={TIP} /></PieChart>
            </ResponsiveContainer>
          </div>
          <div className="flex flex-col gap-3 mt-4">{statuses.map((s) => (
            <div key={s.status} className="flex items-center justify-between">
              <div className="flex items-center gap-2.5">
                <div className="w-3 h-3 rounded-full" style={{ background: STATUS_COLORS[s.status] || "#CBD5E0" }} />
                <span className="text-[13px] text-gray-600 font-medium capitalize">{s.status.replace("order_", "")}</span>
              </div>
              <div className="text-right">
                <span className="text-[13px] font-bold text-gray-800">{s.count}</span>
                <span className="text-[12px] text-gray-400 ml-1">({((s.count / statusTotal) * 100).toFixed(0)}%)</span>
              </div>
            </div>
          ))}</div>
        </Section>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Section title="Order Volume Heatmap" sub="Hour × Day — peak traffic identification"><Heatmap data={heatmap} /></Section>
        <Section title="Cancellation Rate Trend" sub="Last 48 hours — spike detection">
          <div className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={cancelTrend}>
                <CartesianGrid strokeDasharray="3 3" stroke="#EDF2F7" />
                <XAxis dataKey="timestamp" tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} tickFormatter={timeLabel} interval={7} />
                <YAxis tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} domain={[0, "auto"]} unit="%" />
                <Tooltip contentStyle={TIP} labelFormatter={timeLabel} />
                <Line type="monotone" dataKey="rate" stroke="#E17055" strokeWidth={2.5} dot={false} />
                <Line type="monotone" dataKey={() => 7} stroke="#FDCB6E" strokeWidth={1.5} strokeDasharray="6 3" dot={false} name="Threshold" />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </Section>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Section title="Revenue by State" sub="Top 15 states">
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={states.slice(0, 15)} layout="vertical" margin={{ left: 12 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#EDF2F7" horizontal={false} />
                <XAxis type="number" tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} tickFormatter={fmt} />
                <YAxis type="category" dataKey="state" tick={{ fill: "#4A5568", fontSize: 12, fontWeight: 600 }} width={32} />
                <Tooltip contentStyle={TIP} formatter={(v: number) => fmt(v)} />
                <Bar dataKey="revenue" radius={[0, 8, 8, 0]} barSize={16}>
                  {states.slice(0, 15).map((_, i) => <Cell key={i} fill={i < 3 ? "#4C6FFF" : i < 7 ? "#8B9CFF" : "#C5CDFF"} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Section>

        <Section title="Recent Orders" sub="Latest incoming stream events" className="overflow-auto">
          <table className="data-table">
            <thead><tr>
              <th>Order ID</th><th>State</th>
              <th className="text-right">Amount</th><th className="text-center">Status</th>
              <th className="text-right">Time</th>
            </tr></thead>
            <tbody>{orders.map((o) => (
              <tr key={o.order_id}>
                <td className="font-mono text-[12px] text-gray-500">{o.order_id.slice(0, 8)}</td>
                <td className="text-gray-500 font-medium">{o.customer_state}</td>
                <td className="text-right font-bold text-gray-800">R${o.order_total.toFixed(2)}</td>
                <td className="text-center"><StatusBadge status={o.event_type} /></td>
                <td className="text-right text-gray-400 font-medium">{timeLabel(o.event_timestamp)}</td>
              </tr>
            ))}</tbody>
          </table>
        </Section>
      </div>
    </div>
  );
}
