import { useEffect, useState } from 'react'
import { Bar, BarChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { MapPin } from 'lucide-react'
import type { LocationData } from '../types'

export default function Location() {
  const [data, setData] = useState<LocationData | null>(null)
  const [loading, setLoading] = useState<boolean>(true)

  useEffect(() => {
    fetch('http://localhost:8000/api/locations')
      .then(res => res.json())
      .then((d: LocationData) => {
        setData(d)
        setLoading(false)
      })
      .catch(console.error)
  }, [])

  if (loading || !data) return <div className="animate-pulse h-64 glass-card"></div>

  const formatNumber = (val: number) => new Intl.NumberFormat('en-US').format(val)

  return (
    <div className="space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-700">
      
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        
        {/* Top Pickup Zones */}
        <div className="glass-panel p-6">
          <div className="mb-6 flex justify-between items-center">
            <div>
              <h3 className="text-lg font-bold text-white">Top Pickup Zones</h3>
              <p className="text-sm text-slate-400">By total trips</p>
            </div>
          </div>
          
          <div className="h-[300px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart layout="vertical" data={data.top_pickups} margin={{ top: 0, right: 20, left: 0, bottom: 0 }}>
                <XAxis type="number" hide />
                <YAxis 
                  dataKey="pickup_zone" 
                  type="category" 
                  width={150} 
                  stroke="#475569" 
                  tick={{fill: '#e2e8f0', fontSize: 11}} 
                  tickLine={false}
                  axisLine={false}
                />
                <Tooltip 
                  cursor={{fill: 'rgba(255,255,255,0.05)'}}
                  contentStyle={{ backgroundColor: '#1E293B', border: '1px solid rgba(255,255,255,0.1)', borderRadius: '12px', boxShadow: '0 10px 15px -3px rgba(0,0,0,0.5)' }}
                  itemStyle={{ color: '#fff' }}
                  formatter={(value: unknown) => [formatNumber(value as number), "Trips"]}
                  labelStyle={{ display: 'none' }}
                />
                <Bar 
                  dataKey="trips" 
                  fill="#3B82F6" 
                  radius={[0, 4, 4, 0]}
                  animationDuration={1500}
                />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Top Routes */}
        <div className="glass-panel p-6">
          <div className="mb-6">
            <h3 className="text-lg font-bold text-white">Top Requested Routes</h3>
            <p className="text-sm text-slate-400">Most frequent A to B journeys</p>
          </div>
          
          <div className="space-y-4 h-[300px] overflow-y-auto pr-2">
            {data.top_routes.map((route, idx) => (
              <div key={idx} className="flex items-center justify-between p-3 rounded-lg bg-white/5 border border-white/5 hover:bg-white/10 transition-colors">
                <div className="flex items-center gap-3">
                  <div className="w-8 h-8 rounded-full bg-accent/20 text-accent flex items-center justify-center font-bold text-xs">
                    #{idx + 1}
                  </div>
                  <div>
                    <div className="flex items-center gap-2">
                      <span className="text-sm font-semibold text-white">{route.pickup_zone}</span>
                      <MapPin className="w-3 h-3 text-slate-500" />
                      <span className="text-sm font-semibold text-slate-300">{route.dropoff_zone}</span>
                    </div>
                  </div>
                </div>
                <div className="text-sm font-bold text-primary bg-primary/10 px-3 py-1 rounded-full border border-primary/20">
                  {formatNumber(route.total_trips)}
                </div>
              </div>
            ))}
          </div>
        </div>

      </div>
      
    </div>
  )
}
