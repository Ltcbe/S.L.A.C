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

  if (err) return <main><p style={{color:'red'}}>Erreur : {err}</p></main>
  if (!data) return <main><p>Chargement…</p></main>

  const j = data.journey
  const stops = data.stops as any[]

  return (
    <main>
      <a href="/" style={{opacity:0.7,fontSize:'0.9rem'}}>← Retour</a>

      <div className="card" style={{marginTop:'1rem'}}>
        <div style={{fontSize:'1.2rem',fontWeight:600}}>
          {j.vehicle_name} <span className="badge">{j.status}</span>
        </div>
        <div className="text-sm">
          {new Date(j.planned_departure).toLocaleString()} → {new Date(j.planned_arrival).toLocaleString()}
        </div>
      </div>

      <div className="card" style={{marginTop:'1rem'}}>
        <h2 style={{fontWeight:600,marginBottom:'0.5rem'}}>Arrêts</h2>
        <ol style={{listStyle:'none',padding:0,margin:0,display:'flex',flexDirection:'column',gap:'0.75rem'}}>
          {stops.map((s, idx) => (
            <li key={idx} style={{borderBottom:'1px solid #e5e7eb',paddingBottom:'0.5rem'}}>
              <div style={{display:'flex',justifyContent:'space-between',alignItems:'flex-start'}}>
                <div>
                  <div style={{fontWeight:500}}>{s.station_name}</div>
                  <div className="text-sm">
                    Planifié : {s.planned_departure
                      ? new Date(s.planned_departure).toLocaleString()
                      : s.planned_arrival
                        ? new Date(s.planned_arrival).toLocaleString()
                        : '-'}
                  </div>
                  <div className="text-sm">
                    Réel : {s.realtime_departure
                      ? new Date(s.realtime_departure).toLocaleString()
                      : s.realtime_arrival
                        ? new Date(s.realtime_arrival).toLocaleString()
                        : '-'}
                  </div>
                </div>
                <div className="text-sm" style={{textAlign:'right'}}>
                  {s.left && <span className="badge">parti</span>}{" "}
                  {s.arrived && <span className="badge">arrivé</span>}{" "}
                  {s.is_extra_stop && <span className="badge">extra</span>}{" "}
                  {(s.arrival_canceled || s.departure_canceled) && <span className="badge">annulé</span>}
                </div>
              </div>
            </li>
          ))}
        </ol>
      </div>
    </main>
  )
}
