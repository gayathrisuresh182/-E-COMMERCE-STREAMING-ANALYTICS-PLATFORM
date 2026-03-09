import { useEffect, useState } from "react";
import {
  Bar, BarChart, CartesianGrid, Cell, Legend, Line, LineChart,
  ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from "recharts";
import { api, type CumulativePoint, type Experiment, type ExperimentSummary, type PortfolioItem } from "../api";

const TIP = { background: "#fff", border: "1px solid #E2E8F0", borderRadius: 14, fontSize: 12, boxShadow: "0 10px 30px rgba(0,0,0,0.1)" };
const fmtK = (n: number) => (n >= 1000 ? `$${(n / 1000).toFixed(0)}K` : `$${n}`);
const fmtPct = (n: number) => `${(n * 100).toFixed(2)}%`;

const recCfg: Record<string, { text: string; cls: string }> = {
  LAUNCH: { text: "Launch", cls: "bg-emerald-50 text-emerald-700 border-emerald-200" },
  ITERATE: { text: "Iterate", cls: "bg-amber-50 text-amber-700 border-amber-200" },
  MONITOR: { text: "Monitor", cls: "bg-orange-50 text-orange-700 border-orange-200" },
  DO_NOT_LAUNCH: { text: "Do Not Launch", cls: "bg-red-50 text-red-600 border-red-200" },
};

function RecBadge({ rec }: { rec: string }) {
  const c = recCfg[rec] ?? { text: rec, cls: "bg-gray-100 text-gray-500" };
  return <span className={`px-3 py-1 rounded-lg text-[11px] font-bold border ${c.cls}`}>{c.text}</span>;
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
      <div className="grid grid-cols-3 xl:grid-cols-6 gap-5">{Array.from({ length: 6 }).map((_, i) => <div key={i} className="h-28 rounded-2xl bg-gray-100" />)}</div>
      <div className="h-80 rounded-2xl bg-gray-100" />
    </div>
  );
}

function CIBar({ lift, lower, upper }: { lift: number; lower: number; upper: number }) {
  const minV = Math.min(lower, -5), maxV = Math.max(upper, 5), range = maxV - minV;
  const zero = ((0 - minV) / range) * 100;
  const lP = ((lower - minV) / range) * 100, uP = ((upper - minV) / range) * 100, cP = ((lift - minV) / range) * 100;
  const pos = lift > 0;
  return (
    <div className="relative h-8 mt-2 rounded-lg bg-gray-100">
      <div className="absolute top-0 bottom-0 w-px bg-gray-300" style={{ left: `${zero}%` }} />
      <div className={`absolute top-2.5 h-3 rounded-full ${pos ? "bg-emerald-100" : "bg-red-100"}`} style={{ left: `${lP}%`, width: `${uP - lP}%` }} />
      <div className={`absolute top-1.5 w-3 h-5 rounded-md ${pos ? "bg-emerald-500" : "bg-red-500"}`} style={{ left: `${cP}%`, transform: "translateX(-50%)" }} />
    </div>
  );
}

function SampleBar({ current, needed }: { current: number; needed: number }) {
  const pct = Math.min(100, (current / needed) * 100);
  return (
    <div className="space-y-2">
      <div className="flex justify-between text-[11px] font-medium text-gray-500"><span>{current.toLocaleString()} enrolled</span><span>{needed.toLocaleString()} needed</span></div>
      <div className="h-2.5 rounded-full bg-gray-100 overflow-hidden"><div className={`h-full rounded-full transition-all ${pct >= 100 ? "bg-emerald-500" : "bg-[#4C6FFF]"}`} style={{ width: `${pct}%` }} /></div>
    </div>
  );
}

export default function ABTesting() {
  const [summary, setSummary] = useState<ExperimentSummary | null>(null);
  const [experiments, setExperiments] = useState<Experiment[]>([]);
  const [portfolio, setPortfolio] = useState<PortfolioItem[]>([]);
  const [selected, setSelected] = useState<Experiment | null>(null);
  const [cumulative, setCumulative] = useState<CumulativePoint[]>([]);

  useEffect(() => {
    Promise.all([
      api.experiments.summary().then(setSummary),
      api.experiments.list().then((list) => { setExperiments(list); if (list.length) { setSelected(list[0]); api.experiments.cumulative(list[0].experiment_id).then(setCumulative); } }),
      api.experiments.portfolio().then(setPortfolio),
    ]);
  }, []);

  const pick = (exp: Experiment) => { setSelected(exp); api.experiments.cumulative(exp.experiment_id).then(setCumulative); };
  if (!summary) return <Loading />;

  return (
    <div className="space-y-7">
      <div className="anim-fade">
        <h2 className="text-[22px] font-extrabold text-gray-900 tracking-tight">A/B Testing Hub</h2>
        <p className="text-[13px] text-gray-400 mt-1 font-medium">Experiment portfolio — statistical rigor and revenue impact</p>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-5">
        {[
          { l: "Active Experiments", v: summary.total_experiments, ac: "#4C6FFF" },
          { l: "Significant", v: summary.significant, s: `${((summary.significant / summary.total_experiments) * 100).toFixed(0)}% hit-rate`, ac: "#00B894" },
          { l: "Ready to Launch", v: summary.launch_ready, ac: "#6C5CE7" },
          { l: "Revenue Impact", v: `$${(summary.total_revenue_impact / 1000).toFixed(0)}K`, s: "/month projected", ac: "#FDCB6E" },
          { l: "Avg Sample Size", v: summary.avg_sample_size.toLocaleString(), ac: "#00CEC9" },
          { l: "Avg Duration", v: `${summary.avg_test_duration_days}d`, ac: "#636E72" },
        ].map((k, i) => (
          <div key={k.l} className={`kpi-card p-5 anim-fade anim-d${i + 1}`} style={{ "--accent": k.ac } as React.CSSProperties}>
            <span className="kpi-label">{k.l}</span>
            <div className="kpi-value mt-2">{k.v}</div>
            {k.s && <div className="text-[11px] text-gray-400 font-medium mt-1">{k.s}</div>}
          </div>
        ))}
      </div>

      <Section title="Experiment Portfolio — Lift %" sub="Ranked by conversion rate lift — filled bars = significant">
        <div className="h-80">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={portfolio} layout="vertical" margin={{ left: 20, right: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#EDF2F7" horizontal={false} />
              <XAxis type="number" tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} unit="%" />
              <YAxis type="category" dataKey="experiment_name" tick={{ fill: "#4A5568", fontSize: 11, fontWeight: 600 }} width={150} />
              <Tooltip contentStyle={TIP} />
              <ReferenceLine x={0} stroke="#E2E8F0" strokeWidth={1} />
              <Bar dataKey="lift_percent" name="Lift %" radius={[0, 8, 8, 0]} barSize={16}>
                {portfolio.map((d, i) => <Cell key={i} fill={d.lift_percent > 0 ? (d.significant ? "#00B894" : "#B2F5EA") : (d.significant ? "#E17055" : "#FED7D7")} />)}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </Section>

      <Section title="Experiment Registry" sub="Click a row to drill into experiment details" className="overflow-x-auto">
        <table className="data-table">
          <thead><tr>
            <th>Experiment</th><th className="text-right">Lift %</th>
            <th className="text-right">p-value</th><th className="text-center">Sig.</th>
            <th className="text-right">Revenue Impact</th><th className="text-right">Days</th>
            <th>Rec</th>
          </tr></thead>
          <tbody>{experiments.map((exp) => (
            <tr key={exp.experiment_id} onClick={() => pick(exp)} className={`cursor-pointer transition-colors ${selected?.experiment_id === exp.experiment_id ? "!bg-blue-50" : ""}`}>
              <td className="text-gray-800 font-semibold">{exp.experiment_name}</td>
              <td className={`text-right font-bold ${exp.lift_percent > 0 ? "text-emerald-600" : "text-red-500"}`}>{exp.lift_percent > 0 ? "+" : ""}{exp.lift_percent.toFixed(1)}%</td>
              <td className="text-right text-gray-500 font-mono text-[12px]">{exp.p_value < 0.001 ? "<0.001" : exp.p_value.toFixed(3)}</td>
              <td className="text-center">{exp.significant ? <span className="text-emerald-500 text-lg">●</span> : <span className="text-gray-300">○</span>}</td>
              <td className="text-right text-gray-600 font-semibold">{fmtK(exp.revenue_impact_monthly)}/mo</td>
              <td className="text-right text-gray-400 font-medium">{exp.days_running}</td>
              <td><RecBadge rec={exp.recommendation} /></td>
            </tr>
          ))}</tbody>
        </table>
      </Section>

      {selected && (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <Section title={`Cumulative Conversion — ${selected.experiment_name}`} sub="Daily conversion rate as sample accumulates" className="lg:col-span-2">
            <div className="h-80">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={cumulative}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#EDF2F7" />
                  <XAxis dataKey="day" tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} label={{ value: "Days Since Start", fill: "#94A3B8", fontSize: 11, fontWeight: 500, position: "bottom" }} />
                  <YAxis tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} unit="%" domain={["auto", "auto"]} />
                  <Tooltip contentStyle={TIP} />
                  <Legend iconType="line" wrapperStyle={{ fontSize: 12, fontWeight: 600 }} />
                  <Line type="monotone" dataKey="control" stroke="#CBD5E0" strokeWidth={2} dot={false} />
                  <Line type="monotone" dataKey="treatment" stroke="#4C6FFF" strokeWidth={2.5} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </Section>

          <div className="card p-6 space-y-5">
            <h3 className="section-title">Statistical Details</h3>
            {[selected.control, selected.treatment].map((v) => (
              <div key={v.variant} className={`rounded-xl p-4 border-2 ${v.variant === "treatment" ? "bg-blue-50/50 border-blue-200" : "bg-gray-50 border-gray-200"}`}>
                <div className="kpi-label mb-3 capitalize">{v.variant}</div>
                <div className="grid grid-cols-2 gap-y-2 text-[13px]">
                  <span className="text-gray-400 font-medium">Users</span><span className="text-right text-gray-700 font-semibold">{v.users.toLocaleString()}</span>
                  <span className="text-gray-400 font-medium">Conversions</span><span className="text-right text-gray-700 font-semibold">{v.conversions.toLocaleString()}</span>
                  <span className="text-gray-400 font-medium">CR</span><span className="text-right text-gray-900 font-bold">{fmtPct(v.conversion_rate)}</span>
                  <span className="text-gray-400 font-medium">Revenue</span><span className="text-right text-gray-700 font-semibold">{fmtK(v.revenue)}</span>
                  <span className="text-gray-400 font-medium">AOV</span><span className="text-right text-gray-700 font-semibold">R${v.aov.toFixed(2)}</span>
                </div>
              </div>
            ))}
            <div>
              <div className="kpi-label mb-1">95% Confidence Interval</div>
              <CIBar lift={selected.lift_percent} lower={selected.ci_lower} upper={selected.ci_upper} />
              <div className="flex justify-between text-[11px] text-gray-400 font-medium mt-1.5">
                <span>{selected.ci_lower.toFixed(1)}%</span>
                <span className="font-bold text-gray-900">{selected.lift_percent > 0 ? "+" : ""}{selected.lift_percent.toFixed(1)}%</span>
                <span>{selected.ci_upper.toFixed(1)}%</span>
              </div>
            </div>
            <div>
              <div className="kpi-label mb-1">Sample Size Progress</div>
              <SampleBar current={selected.sample_size_current} needed={selected.sample_size_needed} />
            </div>
            <div className="grid grid-cols-2 gap-4">
              <div className="text-center p-4 rounded-xl bg-gray-50 border-2 border-gray-200">
                <div className="text-[11px] text-gray-400 font-semibold mb-1">p-value</div>
                <div className="text-[20px] font-mono font-extrabold text-gray-900">{selected.p_value < 0.001 ? "<0.001" : selected.p_value.toFixed(4)}</div>
              </div>
              <div className="text-center p-4 rounded-xl bg-gray-50 border-2 border-gray-200">
                <div className="text-[11px] text-gray-400 font-semibold mb-1">Monthly Impact</div>
                <div className={`text-[20px] font-extrabold ${selected.revenue_impact_monthly > 0 ? "text-emerald-600" : "text-red-500"}`}>{fmtK(selected.revenue_impact_monthly)}</div>
              </div>
            </div>
            <RecBadge rec={selected.recommendation} />
          </div>
        </div>
      )}
    </div>
  );
}
