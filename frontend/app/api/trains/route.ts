// --- frontend/app/api/trains/route.ts ---
import { NextRequest, NextResponse } from 'next/server'

const BACKEND_URL = process.env.BACKEND_URL || 'http://backend:8000'

export async function GET(req: NextRequest) {
  const url = new URL(req.url)
  const status = url.searchParams.get('status')
  const target = `${BACKEND_URL}/trains${status ? `?status=${encodeURIComponent(status)}` : ''}`

  const r = await fetch(target, { cache: 'no-store' })
  if (!r.ok) {
    return NextResponse.json({ error: 'upstream', status: r.status }, { status: 502 })
  }
  const data = await r.json()
  return NextResponse.json(data)
}
