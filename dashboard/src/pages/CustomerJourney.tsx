import { useEffect, useState } from "react";
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import {
  api, type CartAbandonment, type DeviceConversion, type DeviceFunnelData, type FunnelByVariant,
  type FunnelStage, type JourneyTransition, type ReferrerConversion, type SessionMetrics, type TimeBucket,
} from "../api";

const TIP = { background: "#fff", border: "1px solid #E2E8F0", borderRadius: 14, fontSize: 12, boxShadow: "0 10px 30px rgba(0,0,0,0.1)" };
const fmtK = (n: number) => n >= 1e6 ? `R$${(n / 1e6).toFixed(1)}M` : n >= 1e3 ? `R$${(n / 1e3).toFixed(1)}K` : `R$${n.toFixed(0)}`;

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
      <div className="grid grid-cols-2 md:grid-cols-4 gap-5">{Array.from({ length: 8 }).map((_, i) => <div key={i} className="h-28 rounded-2xl bg-gray-100" />)}</div>
      <div className="h-52 rounded-2xl bg-gray-100" />
    </div>
  );
}

const FUNNEL_COLORS = ["#4C6FFF", "#6C5CE7", "#A78BFA", "#C084FC", "#00B894"];

function Funnel({ stages }: { stages: FunnelStage[] }) {
  const max = stages[0]?.count ?? 1;
  return (
    <div className="flex flex-col gap-3">
      {stages.map((s, i) => {
        const w = Math.max((s.count / max) * 100, 12);
        return (
          <div key={s.stage} className="flex items-center gap-4">
            <div className="w-32 text-right text-[13px] text-gray-600 shrink-0 font-semibold">{s.stage}</div>
            <div className="flex-1 relative">
              <div className="h-11 rounded-xl flex items-center px-4" style={{ width: `${w}%`, background: FUNNEL_COLORS[i % FUNNEL_COLORS.length] }}>
                <span className="text-[13px] font-bold text-white whitespace-nowrap">{s.count.toLocaleString()} ({s.percentage}%)</span>
              </div>
              {s.revenue !== undefined && s.revenue > 0 && <span className="absolute right-0 top-1/2 -translate-y-1/2 text-[11px] text-gray-400 font-medium">{fmtK(s.revenue)}</span>}
            </div>
            {i > 0 && (
              <div className="w-20 text-right shrink-0"><span className="text-[12px] text-red-500 font-bold">▼ {s.drop_off_rate}%</span></div>
            )}
          </div>
        );
      })}
    </div>
  );
}

function VariantFunnel({ data }: { data: FunnelByVariant }) {
  const stages = data.control.map((s) => s.stage);
  const chartData = stages.map((stage) => ({
    stage,
    control: data.control.find((s) => s.stage === stage)?.percentage ?? 0,
    treatment: data.treatment.find((s) => s.stage === stage)?.percentage ?? 0,
  }));
  return (
    <div>
      <div className="h-72">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={chartData} layout="vertical" margin={{ left: 10 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#EDF2F7" horizontal={false} />
            <XAxis type="number" tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} unit="%" domain={[0, 100]} />
            <YAxis type="category" dataKey="stage" tick={{ fill: "#4A5568", fontSize: 11, fontWeight: 600 }} width={100} />
            <Tooltip contentStyle={TIP} formatter={(v: number) => `${v}%`} />
            <Bar dataKey="control" fill="#CBD5E0" barSize={12} radius={[0, 6, 6, 0]} name="Control" />
            <Bar dataKey="treatment" fill="#4C6FFF" barSize={12} radius={[0, 6, 6, 0]} name="Treatment" />
          </BarChart>
        </ResponsiveContainer>
      </div>
      <div className="flex gap-2 mt-4 flex-wrap">
        {data.improvements.filter((d) => d.improvement_percent !== 0).map((d) => (
          <div key={d.stage} className={`text-[11px] px-3 py-1 rounded-lg border font-bold ${d.improvement_percent > 0 ? "border-emerald-200 text-emerald-700 bg-emerald-50" : "border-red-200 text-red-600 bg-red-50"}`}>
            {d.stage}: {d.improvement_percent > 0 ? "+" : ""}{d.improvement_percent}%
          </div>
        ))}
      </div>
    </div>
  );
}

function DeviceFunnelChart({ data }: { data: DeviceFunnelData }) {
  const devices = Object.entries(data) as [string, FunnelStage[]][];
  const colors: Record<string, string> = { desktop: "#4C6FFF", mobile: "#6C5CE7", tablet: "#FDCB6E" };
  const stages = data.desktop.map((s) => s.stage);
  const chartData = stages.map((stage) => {
    const row: Record<string, string | number> = { stage };
    for (const [device, funnel] of devices) { row[device] = funnel.find((s) => s.stage === stage)?.percentage ?? 0; }
    return row;
  });
  return (
    <div className="h-72">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={chartData} layout="vertical" margin={{ left: 10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#EDF2F7" horizontal={false} />
          <XAxis type="number" tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} unit="%" domain={[0, 100]} />
          <YAxis type="category" dataKey="stage" tick={{ fill: "#4A5568", fontSize: 11, fontWeight: 600 }} width={100} />
          <Tooltip contentStyle={TIP} formatter={(v: number) => `${v}%`} />
          {devices.map(([device]) => <Bar key={device} dataKey={device} fill={colors[device] ?? "#4C6FFF"} barSize={10} radius={[0, 6, 6, 0]} name={device.charAt(0).toUpperCase() + device.slice(1)} />)}
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

function JourneyFlow({ transitions }: { transitions: JourneyTransition[] }) {
  const max = Math.max(...transitions.map((t) => t.value), 1);
  return (
    <div className="space-y-2.5">
      {transitions.map((t, i) => {
        const w = Math.max((t.value / max) * 100, 8);
        const exit = t.target === "exit";
        return (
          <div key={i} className="flex items-center gap-3 text-[13px]">
            <span className="w-28 text-right text-gray-600 shrink-0 truncate font-semibold">{t.source.replace(/_/g, " ")}</span>
            <span className="text-gray-300 text-lg">→</span>
            <div className="flex-1">
              <div className={`h-7 rounded-lg flex items-center px-3 ${exit ? "bg-red-50" : "bg-blue-50"}`} style={{ width: `${w}%` }}>
                <span className={`text-[11px] font-bold ${exit ? "text-red-500" : "text-[#4C6FFF]"}`}>{t.value.toLocaleString()}</span>
              </div>
            </div>
            <span className={`w-28 text-[13px] shrink-0 font-semibold ${exit ? "text-red-400" : "text-gray-500"}`}>{t.target.replace(/_/g, " ")}</span>
          </div>
        );
      })}
    </div>
  );
}

export default function CustomerJourney() {
  const [funnel, setFunnel] = useState<FunnelStage[]>([]);
  const [byVariant, setByVariant] = useState<FunnelByVariant | null>(null);
  const [deviceFunnel, setDeviceFunnel] = useState<DeviceFunnelData | null>(null);
  const [abandonment, setAbandonment] = useState<CartAbandonment | null>(null);
  const [sessionMet, setSessionMet] = useState<SessionMetrics | null>(null);
  const [ttc, setTtc] = useState<TimeBucket[]>([]);
  const [devices, setDevices] = useState<DeviceConversion[]>([]);
  const [referrers, setReferrers] = useState<ReferrerConversion[]>([]);
  const [transitions, setTransitions] = useState<JourneyTransition[]>([]);

  useEffect(() => {
    Promise.all([
      api.funnel.overall().then(setFunnel), api.funnel.byVariant().then(setByVariant),
      api.funnel.deviceFunnel().then(setDeviceFunnel), api.funnel.abandonment().then(setAbandonment),
      api.funnel.sessionMetrics().then(setSessionMet), api.funnel.timeToConvert().then(setTtc),
      api.funnel.byDevice().then(setDevices), api.funnel.byReferrer().then(setReferrers),
      api.funnel.transitions().then(setTransitions),
    ]);
  }, []);

  if (!funnel.length || !abandonment || !sessionMet) return <Loading />;

  const overallCR = ((funnel[funnel.length - 1].count / funnel[0].count) * 100).toFixed(1);

  return (
    <div className="space-y-7">
      <div className="anim-fade">
        <h2 className="text-[22px] font-extrabold text-gray-900 tracking-tight">Customer Journey & Funnel</h2>
        <p className="text-[13px] text-gray-400 mt-1 font-medium">Session-level funnel analytics — identify friction and optimize conversion</p>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 xl:grid-cols-8 gap-4">
        {[
          { l: "Sessions", v: funnel[0]?.count.toLocaleString(), ac: "#4C6FFF" },
          { l: "Overall CR", v: `${overallCR}%`, ac: "#00B894" },
          { l: "Cart Abandonment", v: fmtK(abandonment.abandoned_value), s: `${abandonment.abandonment_rate}% rate`, ac: "#E17055" },
          { l: "Avg Abandoned", v: `R$${abandonment.avg_abandoned_cart_value.toFixed(0)}`, ac: "#FDCB6E" },
          { l: "Recovery Rate", v: `${abandonment.recovery_rate}%`, s: fmtK(abandonment.recovered_revenue), ac: "#6C5CE7" },
          { l: "Median Session", v: `${Math.round(sessionMet.median_session_duration_sec / 60)}m ${sessionMet.median_session_duration_sec % 60}s`, ac: "#00CEC9" },
          { l: "Pages / Session", v: sessionMet.avg_pages_per_session.toFixed(1), ac: "#4C6FFF" },
          { l: "Bounce Rate", v: `${sessionMet.bounce_rate}%`, ac: "#E17055" },
        ].map((k, i) => (
          <div key={k.l} className={`kpi-card p-4 anim-fade anim-d${Math.min(i + 1, 6)}`} style={{ "--accent": k.ac } as React.CSSProperties}>
            <span className="kpi-label text-[10px]">{k.l}</span>
            <div className="kpi-value-sm mt-1.5">{k.v}</div>
            {k.s && <div className="text-[10px] text-gray-400 font-medium mt-1">{k.s}</div>}
          </div>
        ))}
      </div>

      <Section title="Conversion Funnel" sub="Session → Order pipeline with revenue at each stage and drop-off rates">
        <Funnel stages={funnel} />
      </Section>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {deviceFunnel && <Section title="Funnel by Device" sub="Mobile checkout shows highest drop-off"><DeviceFunnelChart data={deviceFunnel} /></Section>}
        {byVariant && <Section title="Control vs Treatment" sub="One-Page Checkout experiment impact"><VariantFunnel data={byVariant} /></Section>}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <Section title="Time to Convert" sub="Minutes from session start to order">
          <div className="h-60">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={ttc}>
                <CartesianGrid strokeDasharray="3 3" stroke="#EDF2F7" />
                <XAxis dataKey="bucket" tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} />
                <YAxis tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} />
                <Tooltip contentStyle={TIP} />
                <Bar dataKey="count" fill="#6C5CE7" radius={[8, 8, 0, 0]} barSize={20} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Section>

        <Section title="Conversion by Device" sub="Revenue per session reveals device efficiency">
          <div className="space-y-5 mt-1">
            {devices.map((d) => (
              <div key={d.device} className="space-y-2">
                <div className="flex justify-between text-[13px]">
                  <span className="text-gray-700 font-bold capitalize">{d.device}</span>
                  <span className="text-gray-500 font-semibold">{(d.conversion_rate * 100).toFixed(1)}% CR</span>
                </div>
                <div className="h-3 rounded-full bg-gray-100 overflow-hidden">
                  <div className="h-full rounded-full bg-gradient-to-r from-[#4C6FFF] to-[#6C5CE7]" style={{ width: `${Math.min(d.conversion_rate * 100 * 3, 100)}%` }} />
                </div>
                <div className="flex justify-between text-[11px] text-gray-400 font-medium">
                  <span>{d.sessions.toLocaleString()} sessions</span>
                  <span>R${d.revenue_per_session.toFixed(2)}/session</span>
                </div>
              </div>
            ))}
          </div>
        </Section>

        <Section title="Conversion by Source" sub="Traffic source quality analysis">
          <div className="h-60">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={referrers} layout="vertical" margin={{ left: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#EDF2F7" horizontal={false} />
                <XAxis type="number" tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} tickFormatter={(v: number) => `${(v * 100).toFixed(0)}%`} />
                <YAxis type="category" dataKey="referrer" tick={{ fill: "#4A5568", fontSize: 12, fontWeight: 600 }} width={60} />
                <Tooltip contentStyle={TIP} formatter={(v: number) => `${(v * 100).toFixed(1)}%`} />
                <Bar dataKey="conversion_rate" fill="#00B894" radius={[0, 8, 8, 0]} barSize={14} name="Conversion Rate" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Section>
      </div>

      <Section title="User Journey Flow" sub="Session transitions — identifies common paths and drop-off points">
        <JourneyFlow transitions={transitions} />
      </Section>
    </div>
  );
}
