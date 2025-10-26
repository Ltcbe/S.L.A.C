'use client'
import { useEffect, useState } from 'react'

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
    <main className="max-w-4xl mx-auto p-6 space-y-6">
      <header className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">SNCB Slac</h1>
        <div className="space-x-2">
          <button className={`badge ${status==='running'?'bg-blue-800':''}`} onClick={() => setStatus('running')}>En cours</button>
          <button className={`badge ${status==='completed'?'bg-blue-800':''}`} onClick={() => setStatus('completed')}>Historique</button>
        </div>
      </header>

      {loading && <p>Chargement…</p>}
      {err && <p className="text-red-400">Erreur : {err}</p>}

      {!loading && !err && (
        <div className="grid gap-3">
          {items.map(j => (
            <a key={j.id} href={`/train/${j.id}`} className="card hover:opacity-90">
              <div className="flex items-center justify-between">
                <div>
                  <div className="text-lg font-semibold">{j.vehicle_name} <span className="badge">{j.status}</span></div>
                  <div className="opacity-80 text-sm">{new Date(j.planned_departure).toLocaleString()} → {new Date(j.planned_arrival).toLocaleString()}</div>
                </div>
                <div className="text-right text-xs opacity-70">{j.vehicle_uri}</div>
              </div>
            </a>
          ))}
          {!items.length && <div className="opacity-70">Aucun train à afficher.</div>}
        </div>
      )}
    </main>
  )
}
