// --- frontend/app/api/trains/[id]/route.ts ---
import { NextResponse } from 'next/server'

const BACKEND_URL = process.env.BACKEND_URL || 'http://backend:8000'

export async function GET(_: Request, ctx: { params: { id: string } }) {
  const id = ctx.params.id
  const r = await fetch(`${BACKEND_URL}/trains/${encodeURIComponent(id)}`, { cache: 'no-store' })
  if (!r.ok) {
    return NextResponse.json({ error: 'upstream', status: r.status }, { status: r.status })
  }
  const data = await r.json()
  return NextResponse.json(data)
}
