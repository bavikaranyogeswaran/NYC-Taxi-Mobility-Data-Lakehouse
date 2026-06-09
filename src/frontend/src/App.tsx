import { useState } from 'react'
import { Activity, BarChart2, MapPin, Menu, Car } from 'lucide-react'
import Overview from './pages/Overview'
import Demand from './pages/Demand'
import Location from './pages/Location'

export default function App() {
  const [activeTab, setActiveTab] = useState<string>('overview')

  const navItems = [
    { id: 'overview', label: 'Executive Overview', icon: Activity },
    { id: 'demand', label: 'Demand Analysis', icon: BarChart2 },
    { id: 'location', label: 'Location Analytics', icon: MapPin },
  ]

  return (
    <div className="flex h-screen w-full bg-background relative overflow-hidden">
      {/* Background Decorative Blobs */}
      <div className="absolute top-[-10%] left-[-10%] w-96 h-96 bg-primary/20 rounded-full blur-3xl" />
      <div className="absolute bottom-[-10%] right-[-10%] w-[30rem] h-[30rem] bg-secondary/10 rounded-full blur-3xl" />
      
      {/* Sidebar */}
      <aside className="w-64 glass-panel m-4 flex flex-col z-10 hidden md:flex">
        <div className="p-6 flex items-center gap-3 border-b border-white/5">
          <div className="bg-gradient-to-br from-primary to-secondary p-2 rounded-xl shadow-lg">
            <Car className="w-6 h-6 text-white" />
          </div>
          <div>
            <h1 className="font-bold text-lg text-white leading-tight">Lakehouse</h1>
            <p className="text-xs text-slate-400">NYC Mobility Data</p>
          </div>
        </div>
        
        <nav className="flex-1 p-4 space-y-2">
          {navItems.map((item) => {
            const Icon = item.icon
            const isActive = activeTab === item.id
            return (
              <button
                key={item.id}
                onClick={() => setActiveTab(item.id)}
                className={`w-full flex items-center gap-3 px-4 py-3 rounded-xl transition-all duration-300 ${
                  isActive 
                    ? 'bg-primary/20 text-white shadow-[0_0_15px_rgba(59,130,246,0.15)] border border-primary/30' 
                    : 'text-slate-400 hover:bg-white/5 hover:text-slate-200'
                }`}
              >
                <Icon className={`w-5 h-5 ${isActive ? 'text-primary' : ''}`} />
                <span className="font-medium">{item.label}</span>
              </button>
            )
          })}
        </nav>
        
        <div className="p-6 border-t border-white/5 text-xs text-slate-500">
          Powered by PySpark & FastAPI
        </div>
      </aside>

      {/* Main Content Area */}
      <main className="flex-1 flex flex-col h-full w-full relative z-10">
        {/* Header (Mobile menu placeholder & Context) */}
        <header className="h-20 flex items-center justify-between px-8">
          <div className="flex items-center gap-4">
            <button className="md:hidden p-2 text-slate-400 hover:text-white glass-panel">
              <Menu className="w-5 h-5" />
            </button>
            <h2 className="text-2xl font-bold text-white tracking-tight">
              {navItems.find(i => i.id === activeTab)?.label}
            </h2>
          </div>
          <div className="flex items-center gap-4">
            <div className="flex items-center gap-2 bg-emerald-500/10 text-emerald-400 px-3 py-1.5 rounded-full border border-emerald-500/20">
              <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
              <span className="text-xs font-semibold uppercase tracking-wider">Live</span>
            </div>
            <div className="w-10 h-10 rounded-full bg-gradient-to-tr from-accent to-secondary p-[2px]">
              <div className="w-full h-full bg-surface rounded-full flex items-center justify-center font-bold text-sm">
                AD
              </div>
            </div>
          </div>
        </header>

        {/* Dynamic Page Rendering */}
        <div className="flex-1 overflow-y-auto p-4 md:p-8 pt-0">
          <div className="max-w-7xl mx-auto space-y-6">
            {activeTab === 'overview' && <Overview />}
            {activeTab === 'demand' && <Demand />}
            {activeTab === 'location' && <Location />}
          </div>
        </div>
      </main>
    </div>
  )
}
