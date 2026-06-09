import { useEffect, useState } from 'react'
import { Bar, BarChart, ResponsiveContainer, Tooltip, XAxis, YAxis, CartesianGrid } from 'recharts'
import type { HourlyDemand } from '../types'

export default function Demand() {
  const [data, setData] = useState<HourlyDemand[] | null>(null)
  const [loading, setLoading] = useState<boolean>(true)

  useEffect(() => {
    fetch('http://localhost:8000/api/demand')
      .then(res => res.json())
      .then((d: HourlyDemand[]) => {
        setData(d)
        setLoading(false)
      })
      .catch(console.error)
  }, [])

  if (loading || !data) return <div className="animate-pulse h-64 glass-card"></div>

  const formatNumber = (val: number) => new Intl.NumberFormat('en-US').format(val)
  const formatHour = (h: number) => {
    const ampm = h >= 12 ? 'PM' : 'AM'
    const hour = h % 12 || 12
    return `${hour} ${ampm}`
  }

  const maxPeak = data.reduce((max, d) => d.total_trips > max.total_trips ? d : max, data[0])

  return (
    <div className="space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-700">
      
      <div className="glass-panel p-6">
        <div className="mb-6 flex justify-between items-end">
          <div>
            <h3 className="text-lg font-bold text-white">Hourly Demand Profile</h3>
            <p className="text-sm text-slate-400">Total trips aggregated by hour of day</p>
          </div>
          <div className="bg-secondary/20 text-secondary px-4 py-2 rounded-lg text-sm font-semibold border border-secondary/30 shadow-[0_0_15px_rgba(139,92,246,0.2)]">
            Peak: {formatHour(maxPeak.pickup_hour)}
          </div>
        </div>
        
        <div className="h-[400px] w-full">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={data} margin={{ top: 20, right: 20, left: 20, bottom: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" vertical={false} opacity={0.5} />
              <defs>
                <linearGradient id="barGradient" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#8B5CF6"/>
                  <stop offset="100%" stopColor="#3B82F6"/>
                </linearGradient>
              </defs>
              <XAxis 
                dataKey="pickup_hour" 
                tickFormatter={formatHour}
                stroke="#475569" 
                tick={{fill: '#94a3b8', fontSize: 12}} 
                tickLine={false}
                axisLine={false}
              />
              <YAxis 
                stroke="#475569" 
                tick={{fill: '#94a3b8', fontSize: 12}}
                tickFormatter={(val) => `${val/1000}k`}
                tickLine={false}
                axisLine={false}
              />
              <Tooltip 
                cursor={{fill: 'rgba(255,255,255,0.05)'}}
                contentStyle={{ backgroundColor: '#1E293B', border: '1px solid rgba(255,255,255,0.1)', borderRadius: '12px', boxShadow: '0 10px 15px -3px rgba(0,0,0,0.5)' }}
                itemStyle={{ color: '#fff' }}
                formatter={(value: unknown) => [formatNumber(value as number), "Trips"]}
                labelFormatter={(label: unknown) => formatHour(label as number)}
                labelStyle={{ color: '#94a3b8', marginBottom: '4px' }}
              />
              <Bar 
                dataKey="total_trips" 
                fill="url(#barGradient)" 
                radius={[6, 6, 0, 0]}
                animationDuration={1500}
              />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
      
    </div>
  )
}
