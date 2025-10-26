'use client'
import { useEffect, useState } from 'react'
import { useParams } from 'next/navigation'

export default function TrainPage() {
  const params = useParams()
  const id = params?.id as string
  const [data, setData] = useState<any>(null)
  const [err, setErr] = useState<string | null>(null)

  useEffect(() => {
    if (!id) return
    setErr(null)
    fetch(`/api/trains/${id}`, { cache: 'no-store' })
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json() })
      .then(setData)
      .catch(e => setErr(e.message || 'Erreur réseau'))
  }, [id])

  if (err) return <main className="max-w-3xl mx-auto p-6">Erreur : {err}</main>
  if (!data) return <main className="max-w-3xl mx-auto p-6">Chargement…</main>

  const j = data.journey
  const stops = data.stops as any[]

  return (
    <main className="max-w-3xl mx-auto p-6 space-y-4">
      <a href="/" className="opacity-70 text-sm">← Retour</a>
      <div className="card">
        <div className="text-xl font-semibold">{j.vehicle_name} <span className="badge">{j.status}</span></div>
        <div className="opacity-80 text-sm">{new Date(j.planned_departure).toLocaleString()} → {new Date(j.planned_arrival).toLocaleString()}</div>
      </div>
      <div className="card">
        <h2 className="font-semibold mb-2">Arrêts</h2>
        <ol className="space-y-2">
          {stops.map((s, idx) => (
            <li key={idx} className="flex items-start justify-between border-b border-slate-700 pb-2">
              <div>
                <div className="font-medium">{s.station_name}</div>
                <div className="text-xs opacity-80">Planifié: {s.planned_departure ? new Date(s.planned_departure).toLocaleString() : s.planned_arrival ? new Date(s.planned_arrival).toLocaleString() : '-'}</div>
                <div className="text-xs opacity-80">Réel: {s.realtime_departure ? new Date(s.realtime_departure).toLocaleString() : s.realtime_arrival ? new Date(s.realtime_arrival).toLocaleString() : '-'}</div>
              </div>
              <div className="text-right text-xs space-x-1">
                {s.left && <span className="badge">parti</span>}
                {s.arrived && <span className="badge">arrivé</span>}
                {s.is_extra_stop && <span className="badge">extra</span>}
                {(s.arrival_canceled || s.departure_canceled) && <span className="badge">annulé</span>}
              </div>
            </li>
          ))}
        </ol>
      </div>
    </main>
  )
}
