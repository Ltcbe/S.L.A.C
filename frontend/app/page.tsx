'use client'
import { useEffect, useState } from 'react'
import './globals.css'

type Journey = {
  id: number
  vehicle_name: string
  vehicle_uri: string
  service_date: string
  planned_departure: string
  planned_arrival: string
  status: 'running' | 'completed'
}

export default function Home() {
  const [status, setStatus] = useState<'running'|'completed'|''>('running')
  const [items, setItems] = useState<Journey[]>([])
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState<string | null>(null)

  const fetchList = async (s: 'running'|'completed'|'') => {
    setLoading(true); setErr(null)
    const q = s ? `?status=${s}` : ''
    try {
      const res = await fetch(`/api/trains${q}`, { cache: 'no-store' })
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const data = await res.json()
      setItems(data)
    } catch (e:any) {
      setErr(e.message || 'Erreur réseau')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { fetchList(status) }, [status])

  return (
    <main>
      <header style={{display:'flex',justifyContent:'space-between',alignItems:'center',marginBottom:'1.5rem'}}>
        <h1>SNCB Slac</h1>
        <div style={{display:'flex',gap:'0.5rem'}}>
          <button className={status==='running'?'bg-blue-800 badge':'badge'} onClick={() => setStatus('running')}>
            En cours
          </button>
          <button className={status==='completed'?'bg-blue-800 badge':'badge'} onClick={() => setStatus('completed')}>
            Historique
          </button>
        </div>
      </header>

      {loading && <p>Chargement…</p>}
      {err && <p style={{color:'red'}}>Erreur : {err}</p>}

      {!loading && !err && (
        <div style={{display:'flex',flexDirection:'column',gap:'0.75rem'}}>
          {items.map(j => (
            <a key={j.id} href={`/train/${j.id}`} className="card" style={{textDecoration:'none'}}>
              <div style={{display:'flex',justifyContent:'space-between',alignItems:'center'}}>
                <div>
                  <div style={{fontSize:'1.1rem',fontWeight:600}}>
                    {j.vehicle_name} <span className="badge">{j.status}</span>
                  </div>
                  <div className="text-sm">
                    {new Date(j.planned_departure).toLocaleString()} → {new Date(j.planned_arrival).toLocaleString()}
                  </div>
                </div>
                <div className="text-sm opacity-70">{j.vehicle_uri}</div>
              </div>
            </a>
          ))}
          {!items.length && <div className="opacity-70">Aucun train à afficher.</div>}
        </div>
      )}
    </main>
  )
}
