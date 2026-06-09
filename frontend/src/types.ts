export interface DailyRevenue {
  pickup_date: string;
  total_revenue: number;
  total_trips: number;
}

export interface OverviewSummary {
  total_trips: number;
  total_revenue: number;
  average_fare: number;
  average_duration_minutes: number;
}

export interface OverviewData {
  summary: OverviewSummary;
  daily_trend: DailyRevenue[];
}

export interface HourlyDemand {
  pickup_hour: number;
  total_trips: number;
  avg_duration_minutes: number;
  avg_speed_mph: number;
}

export interface TopPickupZone {
  pickup_zone: string;
  trips: number;
}

export interface TopRoute {
  pickup_zone: string;
  dropoff_zone: string;
  total_trips: number;
}

export interface LocationData {
  top_pickups: TopPickupZone[];
  top_routes: TopRoute[];
}
