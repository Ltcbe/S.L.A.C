// --- frontend/app/layout.tsx ---
import './globals.css'
import type { Metadata } from 'next'

export const metadata: Metadata = {
  title: 'SNCB Slac',
  description: 'Trajets Tournai ↔ Bruxelles-Central (iRail), en cours et archivés',
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="fr">
      <body>{children}</body>
    </html>
  )
}
