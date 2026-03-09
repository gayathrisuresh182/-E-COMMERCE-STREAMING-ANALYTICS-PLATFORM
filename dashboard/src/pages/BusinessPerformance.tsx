import { useEffect, useState } from "react";
import {
  Area, AreaChart, Bar, BarChart, CartesianGrid, Cell, Legend,
  Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from "recharts";
import {
  api, type AovBucket, type BusinessKpis, type CategoryTrendWeek,
  type CohortRow, type DailyTrend, type StateGmv, type TopProduct, type WowDay,
} from "../api";

const TIP = { background: "#fff", border: "1px solid #E2E8F0", borderRadius: 14, fontSize: 12, boxShadow: "0 10px 30px rgba(0,0,0,0.1)" };
const fmtM = (n: number) => n >= 1e6 ? `R$${(n / 1e6).toFixed(2)}M` : n >= 1e3 ? `R$${(n / 1e3).toFixed(1)}K` : `R$${n.toFixed(0)}`;
const CAT_COLORS = ["#4C6FFF", "#00B894", "#6C5CE7", "#FDCB6E", "#E17055", "#00CEC9"];

function Spark({ data, color = "#4C6FFF" }: { data: number[]; color?: string }) {
  const pts = data.map((v, i) => ({ v, i }));
  const id = `bsp-${color.replace("#", "")}`;
  return (
    <div className="h-9 w-28">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={pts} margin={{ top: 2, right: 0, bottom: 0, left: 0 }}>
          <defs><linearGradient id={id} x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor={color} stopOpacity={0.2} /><stop offset="100%" stopColor={color} stopOpacity={0} /></linearGradient></defs>
          <Area type="monotone" dataKey="v" stroke={color} strokeWidth={2} fill={`url(#${id})`} dot={false} />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
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
      <div className="grid grid-cols-2 md:grid-cols-5 gap-5">{Array.from({ length: 10 }).map((_, i) => <div key={i} className="h-28 rounded-2xl bg-gray-100" />)}</div>
      <div className="h-80 rounded-2xl bg-gray-100" />
    </div>
  );
}

function CohortHeatmap({ data }: { data: CohortRow[] }) {
  const periods = ["Month 0", "Month 1", "Month 2", "Month 3", "Month 4", "Month 5"];
  const maxVal = Math.max(...data.flatMap((r) => periods.map((p) => (r[p] as number | null) ?? 0)));
  const cellColor = (val: number | null) => {
    if (val === null) return "bg-gray-50 text-gray-300";
    const ratio = maxVal > 0 ? val / maxVal : 0;
    if (ratio >= 0.8) return "bg-[#4C6FFF] text-white";
    if (ratio >= 0.5) return "bg-[#4C6FFF]/60 text-white";
    if (ratio >= 0.2) return "bg-[#4C6FFF]/25 text-[#4C6FFF]";
    if (ratio > 0) return "bg-[#4C6FFF]/10 text-[#4C6FFF]";
    return "bg-gray-50 text-gray-400";
  };
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-[12px]">
        <thead><tr>
          <th className="text-left py-2.5 text-[11px] text-gray-400 font-semibold uppercase tracking-wide">Cohort</th>
          <th className="text-center py-2.5 text-[11px] text-gray-400 font-semibold uppercase tracking-wide">Size</th>
          {periods.map((p) => <th key={p} className="text-center py-2.5 text-[11px] text-gray-400 font-semibold uppercase tracking-wide">{p}</th>)}
        </tr></thead>
        <tbody>{data.map((row) => (
          <tr key={row.cohort}>
            <td className="py-1.5 text-gray-700 font-semibold">{row.cohort}</td>
            <td className="py-1.5 text-center text-gray-500 font-medium">{(row.cohort_size as number).toLocaleString()}</td>
            {periods.map((p) => {
              const val = row[p] as number | null;
              return (
                <td key={p} className="py-1.5 px-1">
                  {val !== null ? (
                    <div className={`rounded-lg px-2 py-1.5 text-center text-[11px] font-bold ${cellColor(val)}`}>{val.toFixed(1)}%</div>
                  ) : (
                    <div className="rounded-lg px-2 py-1.5 text-center text-gray-300 bg-gray-50 text-[11px]">—</div>
                  )}
                </td>
              );
            })}
          </tr>
        ))}</tbody>
      </table>
    </div>
  );
}

function WowChart({ data }: { data: WowDay[] }) {
  return (
    <div className="h-72">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#EDF2F7" />
          <XAxis dataKey="day" tick={{ fill: "#4A5568", fontSize: 12, fontWeight: 600 }} />
          <YAxis tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} tickFormatter={fmtM} />
          <Tooltip contentStyle={TIP} formatter={(v: number) => fmtM(v)} />
          <Legend wrapperStyle={{ fontSize: 12, fontWeight: 600 }} />
          <Bar dataKey="this_week" fill="#4C6FFF" barSize={16} radius={[8, 8, 0, 0]} name="This Week" />
          <Bar dataKey="last_week" fill="#E2E8F0" barSize={16} radius={[8, 8, 0, 0]} name="Last Week" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

export default function BusinessPerformance() {
  const [kpis, setKpis] = useState<BusinessKpis | null>(null);
  const [trends, setTrends] = useState<DailyTrend[]>([]);
  const [states, setStates] = useState<StateGmv[]>([]);
  const [products, setProducts] = useState<TopProduct[]>([]);
  const [wow, setWow] = useState<WowDay[]>([]);
  const [catTrends, setCatTrends] = useState<CategoryTrendWeek[]>([]);
  const [cohort, setCohort] = useState<CohortRow[]>([]);
  const [aovDist, setAovDist] = useState<AovBucket[]>([]);

  useEffect(() => {
    Promise.all([
      api.business.kpis().then(setKpis), api.business.dailyTrends().then(setTrends),
      api.business.byState().then(setStates), api.business.topProducts().then(setProducts),
      api.business.wow().then(setWow), api.business.categoryTrends().then(setCatTrends),
      api.business.cohortRetention().then(setCohort), api.business.aovDistribution().then(setAovDist),
    ]);
  }, []);

  if (!kpis) return <Loading />;
  const categoryKeys = catTrends.length > 0 ? Object.keys(catTrends[0]).filter((k) => k !== "week") : [];

  return (
    <div className="space-y-7">
      <div className="anim-fade">
        <h2 className="text-[22px] font-extrabold text-gray-900 tracking-tight">Business Performance</h2>
        <p className="text-[13px] text-gray-400 mt-1 font-medium">Executive overview — Lambda architecture: historical batch + real-time stream</p>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 xl:grid-cols-5 gap-5">
        {[
          { l: "Total GMV", v: fmtM(kpis.total_gmv), d: kpis.gmv_growth_pct, sp: kpis.sparklines.gmv, c: "#4C6FFF", ac: "#4C6FFF" },
          { l: "GMV (MTD)", v: fmtM(kpis.gmv_mtd), ac: "#6C5CE7" },
          { l: "Total Orders", v: kpis.total_orders.toLocaleString(), d: kpis.order_growth_pct, sp: kpis.sparklines.orders, c: "#00B894", ac: "#00B894" },
          { l: "AOV", v: `R$${kpis.aov.toFixed(2)}`, sp: kpis.sparklines.aov, c: "#6C5CE7", ac: "#6C5CE7" },
          { l: "Items / Order", v: kpis.items_per_order.toFixed(2), ac: "#FDCB6E" },
          { l: "Repeat Rate", v: `${kpis.repeat_purchase_rate}%`, ac: "#00CEC9" },
          { l: "On-Time Delivery", v: `${kpis.on_time_delivery_pct}%`, ac: "#00B894" },
          { l: "Avg Delivery", v: `${kpis.avg_delivery_days}d`, ac: "#636E72" },
          { l: "Review Rate", v: `${kpis.review_rate}%`, ac: "#FDCB6E" },
          { l: "Avg Review", v: `★ ${kpis.avg_review_score.toFixed(2)}`, ac: "#F6AD55" },
        ].map((k, i) => (
          <div key={k.l} className={`kpi-card p-5 anim-fade anim-d${Math.min(i + 1, 6)}`} style={{ "--accent": k.ac } as React.CSSProperties}>
            <span className="kpi-label">{k.l}</span>
            <div className="flex items-end justify-between mt-2">
              <span className="kpi-value-sm">{k.v}</span>
              {k.sp && <Spark data={k.sp} color={k.c} />}
            </div>
            {k.d !== undefined && (
              <div className="flex items-center gap-1.5 mt-2">
                <span className={`text-[12px] font-bold ${k.d >= 0 ? "text-emerald-600" : "text-red-500"}`}>{k.d >= 0 ? "+" : ""}{k.d.toFixed(1)}%</span>
                <span className="text-[11px] text-gray-400">vs prev month</span>
              </div>
            )}
          </div>
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Section title="Daily GMV with 7-Day MA" sub="90-day trend — moving average smooths daily variance">
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={trends}>
                <defs><linearGradient id="gmvG" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#4C6FFF" stopOpacity={0.08} /><stop offset="100%" stopColor="#4C6FFF" stopOpacity={0} /></linearGradient></defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#EDF2F7" />
                <XAxis dataKey="date" tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} tickFormatter={(d: string) => d.slice(5)} interval={6} />
                <YAxis tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} tickFormatter={fmtM} />
                <Tooltip contentStyle={TIP} formatter={(v: number) => fmtM(v)} />
                <Area type="monotone" dataKey="gmv" stroke="#C5CDFF" strokeWidth={1} fill="url(#gmvG)" dot={false} name="Daily GMV" />
                <Line type="monotone" dataKey="gmv_ma7" stroke="#4C6FFF" strokeWidth={2.5} dot={false} name="7-Day MA" />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </Section>

        <Section title="Orders by Status" sub="Stacked area — fulfillment pipeline health">
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={trends}>
                <CartesianGrid strokeDasharray="3 3" stroke="#EDF2F7" />
                <XAxis dataKey="date" tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} tickFormatter={(d: string) => d.slice(5)} interval={6} />
                <YAxis tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} />
                <Tooltip contentStyle={TIP} />
                <Legend wrapperStyle={{ fontSize: 12, fontWeight: 600 }} />
                <Area type="monotone" dataKey="delivered" stackId="1" stroke="#00B894" fill="#00B894" fillOpacity={0.35} />
                <Area type="monotone" dataKey="shipped" stackId="1" stroke="#4C6FFF" fill="#4C6FFF" fillOpacity={0.25} />
                <Area type="monotone" dataKey="processing" stackId="1" stroke="#FDCB6E" fill="#FDCB6E" fillOpacity={0.3} />
                <Area type="monotone" dataKey="canceled" stackId="1" stroke="#E17055" fill="#E17055" fillOpacity={0.2} />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </Section>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Section title="Revenue by Category Over Time" sub="12-week trends — growing vs declining categories">
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={catTrends}>
                <CartesianGrid strokeDasharray="3 3" stroke="#EDF2F7" />
                <XAxis dataKey="week" tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} tickFormatter={(d: string) => d.slice(5)} />
                <YAxis tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} tickFormatter={fmtM} />
                <Tooltip contentStyle={TIP} formatter={(v: number) => fmtM(v)} />
                <Legend wrapperStyle={{ fontSize: 12, fontWeight: 500 }} />
                {categoryKeys.map((key, i) => <Line key={key} type="monotone" dataKey={key} stroke={CAT_COLORS[i % CAT_COLORS.length]} strokeWidth={2.5} dot={false} />)}
              </LineChart>
            </ResponsiveContainer>
          </div>
        </Section>

        <Section title="AOV Distribution" sub="Order value histogram — basket size segmentation">
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={aovDist}>
                <CartesianGrid strokeDasharray="3 3" stroke="#EDF2F7" />
                <XAxis dataKey="bucket" tick={{ fill: "#4A5568", fontSize: 11, fontWeight: 600 }} />
                <YAxis tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} tickFormatter={(v: number) => `${(v / 1000).toFixed(0)}K`} />
                <Tooltip contentStyle={TIP} />
                <Bar dataKey="count" name="Orders" radius={[8, 8, 0, 0]} barSize={30}>
                  {aovDist.map((_, i) => <Cell key={i} fill={i <= 2 ? "#4C6FFF" : i <= 3 ? "#6C5CE7" : "#A78BFA"} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Section>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Section title="Week-over-Week Comparison" sub="Two most recent full weeks of GMV by day"><WowChart data={wow} /></Section>
        <Section title="Cohort Retention" sub="Monthly cohort repeat purchase rates"><CohortHeatmap data={cohort} /></Section>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Section title="GMV by State" sub="Top 15 states by revenue">
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={states.slice(0, 15)} layout="vertical" margin={{ left: 12 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#EDF2F7" horizontal={false} />
                <XAxis type="number" tick={{ fill: "#94A3B8", fontSize: 11, fontWeight: 500 }} tickFormatter={fmtM} />
                <YAxis type="category" dataKey="state" tick={{ fill: "#4A5568", fontSize: 12, fontWeight: 600 }} width={32} />
                <Tooltip contentStyle={TIP} formatter={(v: number) => fmtM(v)} />
                <Bar dataKey="gmv" radius={[0, 8, 8, 0]} barSize={14}>
                  {states.slice(0, 15).map((_, i) => <Cell key={i} fill={i < 3 ? "#4C6FFF" : i < 7 ? "#8B9CFF" : "#C5CDFF"} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Section>

        <Section title="Top Product Categories" sub="Ranked by revenue" className="overflow-x-auto">
          <table className="data-table">
            <thead><tr>
              <th>#</th><th>Category</th>
              <th className="text-right">Units</th><th className="text-right">Revenue</th>
              <th className="text-right">Avg Price</th><th className="text-right">Review</th>
              <th className="text-right">Share</th>
            </tr></thead>
            <tbody>{products.map((p) => (
              <tr key={p.rank}>
                <td className="text-gray-400 font-bold">{p.rank}</td>
                <td className="text-gray-800 font-semibold">{p.category}</td>
                <td className="text-right text-gray-500 font-medium">{p.units_sold.toLocaleString()}</td>
                <td className="text-right text-gray-800 font-bold">{fmtM(p.revenue)}</td>
                <td className="text-right text-gray-500 font-medium">R${p.avg_price.toFixed(0)}</td>
                <td className="text-right text-amber-500 font-bold">★ {p.review_score}</td>
                <td className="text-right">
                  <div className="flex items-center justify-end gap-2">
                    <div className="w-14 h-2 rounded-full bg-gray-100 overflow-hidden"><div className="h-full rounded-full bg-[#4C6FFF]" style={{ width: `${Math.min(p.share_pct * 5, 100)}%` }} /></div>
                    <span className="text-gray-500 font-semibold text-[11px] w-8 text-right">{p.share_pct}%</span>
                  </div>
                </td>
              </tr>
            ))}</tbody>
          </table>
        </Section>
      </div>
    </div>
  );
}
