import { useEffect, useState } from 'react'
import { Area, AreaChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { ArrowUpRight, Banknote, Clock, Navigation } from 'lucide-react'
import type { OverviewData } from '../types'

export default function Overview() {
  const [data, setData] = useState<OverviewData | null>(null)
  const [loading, setLoading] = useState<boolean>(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    fetch('http://localhost:8000/api/overview')
      .then(res => {
        if (!res.ok) throw new Error(`API error: ${res.status}`)
        return res.json()
      })
      .then((d: OverviewData) => {
        setData(d)
        setLoading(false)
      })
      .catch(err => {
        console.error(err)
        setError(err.message)
        setLoading(false)
      })
  }, [])

  if (loading) return <div className="animate-pulse h-64 glass-card"></div>
  if (error || !data?.summary) return (
    <div className="glass-card p-6 text-slate-400 text-sm">
      {error ? `Failed to load data: ${error}` : 'No data available'}
    </div>
  )

  const formatCurrency = (val: number) => new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(val)
  const formatNumber = (val: number) => new Intl.NumberFormat('en-US').format(val)

  return (
    <div className="space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-700">
      
      {/* Top KPI Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        
        {/* Card 1: Revenue */}
        <div className="glass-card p-6 relative overflow-hidden group">
          <div className="absolute top-0 right-0 w-32 h-32 bg-primary/10 rounded-full blur-2xl -mr-10 -mt-10 transition-transform group-hover:scale-150" />
          <div className="flex items-start justify-between">
            <div>
              <p className="text-slate-400 font-medium text-sm">Total Revenue</p>
              <h3 className="text-3xl font-bold mt-2 text-white">{formatCurrency(data.summary.total_revenue)}</h3>
            </div>
            <div className="p-3 bg-primary/20 rounded-xl text-primary">
              <Banknote className="w-5 h-5" />
            </div>
          </div>
          <div className="mt-4 flex items-center text-xs font-medium text-emerald-400">
            <ArrowUpRight className="w-4 h-4 mr-1" />
            <span>Top Performing</span>
          </div>
        </div>

        {/* Card 2: Trips */}
        <div className="glass-card p-6 relative overflow-hidden group">
          <div className="absolute top-0 right-0 w-32 h-32 bg-secondary/10 rounded-full blur-2xl -mr-10 -mt-10 transition-transform group-hover:scale-150" />
          <div className="flex items-start justify-between">
            <div>
              <p className="text-slate-400 font-medium text-sm">Total Trips</p>
              <h3 className="text-3xl font-bold mt-2 text-white">{formatNumber(data.summary.total_trips)}</h3>
            </div>
            <div className="p-3 bg-secondary/20 rounded-xl text-secondary">
              <Navigation className="w-5 h-5" />
            </div>
          </div>
          <div className="mt-4 flex items-center text-xs font-medium text-emerald-400">
            <ArrowUpRight className="w-4 h-4 mr-1" />
            <span>Verified Rides</span>
          </div>
        </div>

        {/* Card 3: Avg Fare */}
        <div className="glass-card p-6 relative overflow-hidden group">
          <div className="absolute top-0 right-0 w-32 h-32 bg-accent/10 rounded-full blur-2xl -mr-10 -mt-10 transition-transform group-hover:scale-150" />
          <div className="flex items-start justify-between">
            <div>
              <p className="text-slate-400 font-medium text-sm">Average Fare</p>
              <h3 className="text-3xl font-bold mt-2 text-white">{formatCurrency(data.summary.average_fare)}</h3>
            </div>
            <div className="p-3 bg-accent/20 rounded-xl text-accent">
              <Banknote className="w-5 h-5" />
            </div>
          </div>
          <div className="mt-4 flex items-center text-xs font-medium text-slate-400">
            <span>Per completed trip</span>
          </div>
        </div>

        {/* Card 4: Avg Duration */}
        <div className="glass-card p-6 relative overflow-hidden group">
          <div className="absolute top-0 right-0 w-32 h-32 bg-emerald-500/10 rounded-full blur-2xl -mr-10 -mt-10 transition-transform group-hover:scale-150" />
          <div className="flex items-start justify-between">
            <div>
              <p className="text-slate-400 font-medium text-sm">Avg Duration</p>
              <h3 className="text-3xl font-bold mt-2 text-white">{data.summary.average_duration_minutes.toFixed(1)} <span className="text-xl text-slate-400">min</span></h3>
            </div>
            <div className="p-3 bg-emerald-500/20 rounded-xl text-emerald-400">
              <Clock className="w-5 h-5" />
            </div>
          </div>
          <div className="mt-4 flex items-center text-xs font-medium text-slate-400">
            <span>Time per trip</span>
          </div>
        </div>

      </div>

      {/* Main Chart */}
      <div className="glass-panel p-6">
        <div className="mb-6">
          <h3 className="text-lg font-bold text-white">Revenue Trend (3 Months)</h3>
          <p className="text-sm text-slate-400">Daily gross revenue across all zones</p>
        </div>
        <div className="h-80 w-full">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={data.daily_trend} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="colorRevenue" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#3B82F6" stopOpacity={0.4}/>
                  <stop offset="95%" stopColor="#3B82F6" stopOpacity={0}/>
                </linearGradient>
              </defs>
              <XAxis 
                dataKey="pickup_date" 
                stroke="#475569" 
                tick={{fill: '#94a3b8', fontSize: 12}} 
                tickLine={false}
                axisLine={false}
                minTickGap={30}
              />
              <YAxis 
                stroke="#475569" 
                tick={{fill: '#94a3b8', fontSize: 12}}
                tickFormatter={(val) => `$${val/1000}k`}
                tickLine={false}
                axisLine={false}
              />
              <Tooltip 
                contentStyle={{ backgroundColor: '#1E293B', border: '1px solid rgba(255,255,255,0.1)', borderRadius: '12px', boxShadow: '0 10px 15px -3px rgba(0,0,0,0.5)' }}
                itemStyle={{ color: '#fff' }}
                formatter={(value: unknown) => [formatCurrency(value as number), "Revenue"]}
                labelStyle={{ color: '#94a3b8', marginBottom: '4px' }}
              />
              <Area 
                type="monotone" 
                dataKey="total_revenue" 
                stroke="#3B82F6" 
                strokeWidth={3}
                fillOpacity={1} 
                fill="url(#colorRevenue)" 
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  )
}
