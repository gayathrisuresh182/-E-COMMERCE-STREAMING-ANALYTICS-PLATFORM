const BASE = "/api";

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) throw new Error(`API ${res.status}: ${path}`);
  return res.json();
}

/* ------------------------------------------------------------------ */
/*  Shared                                                             */
/* ------------------------------------------------------------------ */

export interface Sparklines {
  [key: string]: number[];
}

/* ------------------------------------------------------------------ */
/*  Real-Time Operations                                               */
/* ------------------------------------------------------------------ */

export interface RealtimeKpis {
  total_gmv: number;
  total_orders: number;
  avg_order_value: number;
  last_event: string;
  orders_per_minute: number;
  active_sessions: number;
  revenue_per_session: number;
  cancel_rate: number;
  gmv_vs_yesterday: number;
  orders_vs_yesterday: number;
  sparklines: Sparklines;
}

export interface TimeseriesPoint {
  timestamp: string;
  orders: number;
  gmv: number;
  orders_yesterday: number;
  gmv_yesterday: number;
}

export interface HeatmapCell {
  day: string;
  hour: number;
  orders: number;
}

export interface StateData {
  state: string;
  orders: number;
  revenue: number;
}

export interface StatusData {
  status: string;
  count: number;
  color: string;
}

export interface RecentOrder {
  order_id: string;
  customer_id: string;
  customer_state: string;
  order_total: number;
  event_type: string;
  event_timestamp: string;
}

export interface StreamHealth {
  last_event_age_minutes: number;
  status: string;
  events_last_hour: number;
  events_last_24h: number;
  last_updated: string;
}

export interface CancellationPoint {
  timestamp: string;
  rate: number;
}

/* ------------------------------------------------------------------ */
/*  A/B Testing                                                        */
/* ------------------------------------------------------------------ */

export interface ExperimentSummary {
  total_experiments: number;
  significant: number;
  launch_ready: number;
  total_revenue_impact: number;
  avg_sample_size: number;
  avg_test_duration_days: number;
}

export interface VariantData {
  variant: string;
  users: number;
  conversions: number;
  conversion_rate: number;
  revenue: number;
  aov: number;
}

export interface Experiment {
  experiment_id: string;
  experiment_name: string;
  p_value: number;
  significant: boolean;
  recommendation: string;
  revenue_impact_monthly: number;
  lift_percent: number;
  ci_lower: number;
  ci_upper: number;
  sample_size_needed: number;
  sample_size_current: number;
  days_running: number;
  control: VariantData;
  treatment: VariantData;
}

export interface CumulativePoint {
  day: number;
  control: number;
  treatment: number;
}

export interface PortfolioItem {
  experiment_name: string;
  lift_percent: number;
  ci_lower: number;
  ci_upper: number;
  significant: boolean;
  recommendation: string;
}

/* ------------------------------------------------------------------ */
/*  Customer Journey / Funnel                                          */
/* ------------------------------------------------------------------ */

export interface FunnelStage {
  stage: string;
  count: number;
  percentage: number;
  drop_off_rate: number;
  revenue?: number;
}

export interface FunnelByVariant {
  experiment_id: string;
  control: FunnelStage[];
  treatment: FunnelStage[];
  improvements: { stage: string; improvement_percent: number }[];
}

export interface DeviceFunnelData {
  desktop: FunnelStage[];
  mobile: FunnelStage[];
  tablet: FunnelStage[];
}

export interface CartAbandonment {
  abandoned_carts: number;
  abandoned_value: number;
  abandonment_rate: number;
  top_abandon_stage: string;
  avg_abandoned_cart_value: number;
  recovery_rate: number;
  recovered_revenue: number;
}

export interface SessionMetrics {
  median_session_duration_sec: number;
  avg_pages_per_session: number;
  bounce_rate: number;
  return_visit_conversion: number;
}

export interface TimeBucket {
  bucket: string;
  minutes_start: number;
  count: number;
}

export interface DeviceConversion {
  device: string;
  sessions: number;
  orders: number;
  conversion_rate: number;
  revenue_per_session: number;
}

export interface ReferrerConversion {
  referrer: string;
  sessions: number;
  orders: number;
  conversion_rate: number;
}

export interface JourneyTransition {
  source: string;
  target: string;
  value: number;
}

/* ------------------------------------------------------------------ */
/*  Business Performance                                               */
/* ------------------------------------------------------------------ */

export interface BusinessKpis {
  total_gmv: number;
  gmv_mtd: number;
  gmv_today: number;
  gmv_growth_pct: number;
  total_orders: number;
  orders_mtd: number;
  orders_today: number;
  order_growth_pct: number;
  aov: number;
  items_per_order: number;
  repeat_purchase_rate: number;
  avg_delivery_days: number;
  on_time_delivery_pct: number;
  review_rate: number;
  avg_review_score: number;
  sparklines: Sparklines;
}

export interface DailyTrend {
  date: string;
  gmv: number;
  gmv_ma7: number;
  orders: number;
  aov: number;
  delivered: number;
  shipped: number;
  processing: number;
  canceled: number;
}

export interface WowDay {
  day: string;
  this_week: number;
  last_week: number;
  delta_pct: number;
}

export interface CategoryTrendWeek {
  week: string;
  [category: string]: string | number;
}

export interface CohortRow {
  cohort: string;
  cohort_size: number;
  [period: string]: string | number | null;
}

export interface AovBucket {
  bucket: string;
  count: number;
  pct: number;
}

export interface StateGmv {
  state: string;
  gmv: number;
  orders: number;
  aov: number;
}

export interface TopProduct {
  rank: number;
  category: string;
  units_sold: number;
  revenue: number;
  avg_price: number;
  review_score: number;
  share_pct: number;
}

export interface CategoryRevenue {
  category: string;
  revenue: number;
}

/* ------------------------------------------------------------------ */
/*  Data Quality & Monitoring                                          */
/* ------------------------------------------------------------------ */

export interface KafkaConsumer {
  consumer_group: string;
  topics: string;
  total_lag: number;
  partitions: number;
  status: string;
  measured_at: string;
}

export interface MongoCollection {
  collection: string;
  label: string;
  document_count: number;
  newest_timestamp: string;
}

export interface Alert {
  timestamp: string;
  type: string;
  severity: string;
  message: string;
  status: string;
}

export interface LagHistoryPoint {
  timestamp: string;
  [consumer: string]: string | number;
}

export interface ThroughputPoint {
  timestamp: string;
  orders_stream: number;
  clickstream: number;
  deliveries_stream: number;
}

export interface SlaDay {
  date: string;
  sla_met: boolean;
  duration_min: number;
  sla_target_min: number;
}

export interface DataFreshness {
  dataset: string;
  last_updated: string;
  update_frequency: string;
  sla_hours: number;
  status: string;
}

export interface HealthScore {
  score: number;
  healthy_consumers: number;
  total_consumers: number;
  open_alerts: number;
  status: string;
}

/* ------------------------------------------------------------------ */
/*  API client                                                         */
/* ------------------------------------------------------------------ */

export const api = {
  realtime: {
    kpis: () => get<RealtimeKpis>("/realtime/kpis"),
    timeseries: (hours = 24) => get<TimeseriesPoint[]>(`/realtime/timeseries?hours=${hours}`),
    ordersByState: () => get<StateData[]>("/realtime/orders-by-state"),
    ordersByStatus: () => get<StatusData[]>("/realtime/orders-by-status"),
    recentOrders: (limit = 15) => get<RecentOrder[]>(`/realtime/recent-orders?limit=${limit}`),
    streamHealth: () => get<StreamHealth>("/realtime/stream-health"),
    heatmap: () => get<HeatmapCell[]>("/realtime/heatmap"),
    cancellationTrend: () => get<CancellationPoint[]>("/realtime/cancellation-trend"),
  },
  experiments: {
    summary: () => get<ExperimentSummary>("/experiments/summary"),
    list: () => get<Experiment[]>("/experiments/list"),
    detail: (id: string) => get<Experiment>(`/experiments/${id}`),
    cumulative: (id = "exp_001") => get<CumulativePoint[]>(`/experiments/cumulative?experiment_id=${id}`),
    portfolio: () => get<PortfolioItem[]>("/experiments/portfolio"),
  },
  funnel: {
    overall: () => get<FunnelStage[]>("/funnel/overall"),
    byVariant: (id = "exp_001") => get<FunnelByVariant>(`/funnel/by-variant?experiment_id=${id}`),
    byDevice: () => get<DeviceConversion[]>("/funnel/by-device"),
    byReferrer: () => get<ReferrerConversion[]>("/funnel/by-referrer"),
    timeToConvert: () => get<TimeBucket[]>("/funnel/time-to-convert"),
    transitions: () => get<JourneyTransition[]>("/funnel/transitions"),
    deviceFunnel: () => get<DeviceFunnelData>("/funnel/device-funnel"),
    abandonment: () => get<CartAbandonment>("/funnel/abandonment"),
    sessionMetrics: () => get<SessionMetrics>("/funnel/session-metrics"),
  },
  business: {
    kpis: () => get<BusinessKpis>("/business/kpis"),
    dailyTrends: (days = 90) => get<DailyTrend[]>(`/business/daily-trends?days=${days}`),
    byState: () => get<StateGmv[]>("/business/by-state"),
    topProducts: (limit = 15) => get<TopProduct[]>(`/business/top-products?limit=${limit}`),
    revenueByCategory: () => get<CategoryRevenue[]>("/business/revenue-by-category"),
    wow: () => get<WowDay[]>("/business/wow"),
    categoryTrends: () => get<CategoryTrendWeek[]>("/business/category-trends"),
    cohortRetention: () => get<CohortRow[]>("/business/cohort-retention"),
    aovDistribution: () => get<AovBucket[]>("/business/aov-distribution"),
  },
  monitoring: {
    consumers: () => get<KafkaConsumer[]>("/monitoring/consumers"),
    collections: () => get<MongoCollection[]>("/monitoring/collections"),
    alerts: () => get<Alert[]>("/monitoring/alerts"),
    lagHistory: (hours = 24) => get<LagHistoryPoint[]>(`/monitoring/lag-history?hours=${hours}`),
    healthScore: () => get<HealthScore>("/monitoring/health-score"),
    throughput: (hours = 24) => get<ThroughputPoint[]>(`/monitoring/throughput?hours=${hours}`),
    slaCompliance: () => get<SlaDay[]>("/monitoring/sla-compliance"),
    dataFreshness: () => get<DataFreshness[]>("/monitoring/data-freshness"),
  },
};
