import { Link } from "react-router-dom";

const DASHBOARDS = [
  { to: "/realtime", title: "Real-Time Operations", desc: "Live orders, GMV tracking, traffic heatmap, cancellation monitoring", color: "#4C6FFF", bg: "bg-blue-50", iconColor: "text-blue-600", icon: BoltSVG, badge: "LIVE", badgeCls: "bg-emerald-50 text-emerald-700 border-emerald-200" },
  { to: "/experiments", title: "A/B Testing Hub", desc: "Portfolio view, cumulative conversion, confidence intervals, revenue impact", color: "#6C5CE7", bg: "bg-purple-50", iconColor: "text-purple-600", icon: BeakerSVG, badge: "STATISTICAL", badgeCls: "bg-purple-50 text-purple-700 border-purple-200" },
  { to: "/funnel", title: "Customer Journey", desc: "Funnel by device, cart abandonment, session analytics, source attribution", color: "#00B894", bg: "bg-emerald-50", iconColor: "text-emerald-600", icon: FunnelSVG, badge: "FUNNEL", badgeCls: "bg-emerald-50 text-emerald-700 border-emerald-200" },
  { to: "/business", title: "Business Performance", desc: "7-day MA trends, cohort retention, AOV distribution, category revenue", color: "#FDCB6E", bg: "bg-amber-50", iconColor: "text-amber-600", icon: ChartSVG, badge: "LAMBDA", badgeCls: "bg-amber-50 text-amber-700 border-amber-200" },
  { to: "/quality", title: "Data Quality & Pipeline", desc: "Consumer lag, SLA compliance, data freshness, pipeline alerts", color: "#E17055", bg: "bg-red-50", iconColor: "text-red-500", icon: ShieldSVG, badge: "MONITORING", badgeCls: "bg-red-50 text-red-600 border-red-200" },
];

const TECH = [
  { name: "Apache Kafka",  cls: "border-blue-200 text-blue-700 bg-blue-50" },
  { name: "Dagster",       cls: "border-emerald-200 text-emerald-700 bg-emerald-50" },
  { name: "BigQuery",      cls: "border-yellow-200 text-yellow-700 bg-yellow-50" },
  { name: "MongoDB",       cls: "border-green-200 text-green-700 bg-green-50" },
  { name: "Python",        cls: "border-indigo-200 text-indigo-700 bg-indigo-50" },
  { name: "FastAPI",       cls: "border-teal-200 text-teal-700 bg-teal-50" },
  { name: "React + TS",    cls: "border-cyan-200 text-cyan-700 bg-cyan-50" },
  { name: "Recharts",      cls: "border-violet-200 text-violet-700 bg-violet-50" },
];

const ARCH = [
  { label: "Olist CSV" },
  { label: "Kafka Producers" },
  { label: "Apache Kafka", cls: "text-blue-700 bg-blue-50 border-blue-200 font-semibold" },
  { label: "Consumers" },
  { label: "MongoDB", cls: "text-green-700 bg-green-50 border-green-200 font-semibold" },
  { label: "Dagster", cls: "text-amber-700 bg-amber-50 border-amber-200 font-semibold" },
  { label: "BigQuery", cls: "text-purple-700 bg-purple-50 border-purple-200 font-semibold" },
  { label: "React Dashboard", cls: "text-blue-700 bg-blue-50 border-blue-200 font-semibold" },
];

export default function Home() {
  return (
    <div className="max-w-6xl mx-auto space-y-10">
      <div className="anim-fade">
        <h1 className="text-[28px] font-extrabold text-gray-900 tracking-tight">E-Commerce Streaming Analytics</h1>
        <p className="text-[15px] text-gray-500 mt-2 max-w-2xl leading-relaxed">
          Production-grade analytics combining real-time streaming, batch ETL,
          A/B testing, funnel optimization, and pipeline monitoring.
        </p>
      </div>

      <div className="flex flex-wrap gap-2 anim-fade anim-d1">
        {TECH.map((t) => (
          <span key={t.name} className={`px-3 py-1.5 text-[11px] font-bold rounded-full border ${t.cls}`}>{t.name}</span>
        ))}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {DASHBOARDS.map((d, i) => (
          <Link key={d.to} to={d.to} className={`group block rounded-2xl overflow-hidden bg-white shadow-sm hover:shadow-lg transition-all duration-300 anim-fade anim-d${i + 1}`}>
            <div className="h-[4px]" style={{ background: d.color }} />
            <div className="p-6">
              <div className="flex items-start justify-between mb-4">
                <div className={`w-11 h-11 rounded-xl ${d.bg} flex items-center justify-center`}>
                  <d.icon className={`w-5 h-5 ${d.iconColor}`} />
                </div>
                <span className={`text-[9px] font-bold tracking-widest px-2.5 py-1 rounded-full border ${d.badgeCls}`}>{d.badge}</span>
              </div>
              <h2 className="text-[16px] font-bold text-gray-900 group-hover:text-[#4C6FFF] transition-colors">{d.title}</h2>
              <p className="text-[13px] text-gray-400 mt-2 leading-relaxed">{d.desc}</p>
            </div>
          </Link>
        ))}
      </div>

      <div className="card p-7 anim-fade anim-d5">
        <h3 className="text-[15px] font-bold text-gray-900 mb-5">System Architecture</h3>
        <div className="flex flex-wrap items-center gap-2.5 text-[12px]">
          {ARCH.map((s, i) => (
            <span key={i} className="contents">
              <span className={`px-3.5 py-2 rounded-xl border ${s.cls || "border-gray-200 text-gray-500 bg-gray-50"}`}>{s.label}</span>
              {i < ARCH.length - 1 && <span className="text-gray-300 text-lg">→</span>}
            </span>
          ))}
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 anim-fade anim-d6">
        {[
          { title: "Lambda Architecture", desc: "Batch historical data (2016-2018) merged with real-time stream into unified views for seamless querying.", color: "#4C6FFF" },
          { title: "Statistical Rigor", desc: "A/B testing with p-values, confidence intervals, sample size calculations, and projected revenue impact.", color: "#6C5CE7" },
          { title: "Production Monitoring", desc: "End-to-end observability: Kafka consumer lag, SLA compliance, data freshness, and automated alerting.", color: "#E17055" },
        ].map((c) => (
          <div key={c.title} className="card p-6">
            <div className="w-10 h-1 rounded-full mb-4" style={{ background: c.color }} />
            <h4 className="text-[14px] font-bold text-gray-900 mb-2">{c.title}</h4>
            <p className="text-[13px] text-gray-400 leading-relaxed">{c.desc}</p>
          </div>
        ))}
      </div>
    </div>
  );
}

function BoltSVG({ className }: { className?: string }) {
  return <svg className={className} fill="none" viewBox="0 0 24 24" strokeWidth={1.8} stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" d="m3.75 13.5 10.5-11.25L12 10.5h8.25L9.75 21.75 12 13.5H3.75Z" /></svg>;
}
function BeakerSVG({ className }: { className?: string }) {
  return <svg className={className} fill="none" viewBox="0 0 24 24" strokeWidth={1.8} stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" d="M9.75 3.104v5.714a2.25 2.25 0 0 1-.659 1.591L5.5 14.001a2.25 2.25 0 0 0-.659 1.591v.318c0 1.243 1.007 2.25 2.25 2.25h9.818c1.242 0 2.25-1.007 2.25-2.25v-.318a2.25 2.25 0 0 0-.659-1.59l-3.591-3.593a2.25 2.25 0 0 1-.659-1.59V3.103" /></svg>;
}
function FunnelSVG({ className }: { className?: string }) {
  return <svg className={className} fill="none" viewBox="0 0 24 24" strokeWidth={1.8} stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" d="M12 3c2.755 0 5.455.232 8.083.678.533.09.917.556.917 1.096v1.044a2.25 2.25 0 0 1-.659 1.591l-5.432 5.432a2.25 2.25 0 0 0-.659 1.591v2.927a2.25 2.25 0 0 1-1.244 2.013L9.75 21v-6.568a2.25 2.25 0 0 0-.659-1.591L3.659 7.409A2.25 2.25 0 0 1 3 5.818V4.774c0-.54.384-1.006.917-1.096A48.32 48.32 0 0 1 12 3Z" /></svg>;
}
function ChartSVG({ className }: { className?: string }) {
  return <svg className={className} fill="none" viewBox="0 0 24 24" strokeWidth={1.8} stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 0 1 3 19.875v-6.75ZM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 0 1-1.125-1.125V8.625ZM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 0 1-1.125-1.125V4.125Z" /></svg>;
}
function ShieldSVG({ className }: { className?: string }) {
  return <svg className={className} fill="none" viewBox="0 0 24 24" strokeWidth={1.8} stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" d="M9 12.75 11.25 15 15 9.75m-3-7.036A11.959 11.959 0 0 1 3.598 6 11.99 11.99 0 0 0 3 9.749c0 5.592 3.824 10.29 9 11.623 5.176-1.332 9-6.03 9-11.622 0-1.31-.21-2.571-.598-3.751h-.152c-3.196 0-6.1-1.248-8.25-3.285Z" /></svg>;
}
