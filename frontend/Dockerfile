# ====== Étape deps ======
FROM node:20-bullseye-slim AS deps
WORKDIR /app
# Copie des manifestes pour un cache npm stable
COPY package.json package-lock.json* ./
# Install propre en prod (sans dev) — ajuste si nécessaire
RUN npm ci

# ====== Étape build ======
FROM node:20-bullseye-slim AS builder
WORKDIR /app
ENV NEXT_TELEMETRY_DISABLED=1
# Copie node_modules et sources
COPY --from=deps /app/node_modules ./node_modules
COPY . .
# Build Next en mode standalone
RUN npm run build

# ====== Étape run ======
FROM node:20-bullseye-slim AS runner
WORKDIR /app

ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
# Port et host forcés (évite ECONNREFUSED en 127.0.0.1)
ENV PORT=3000
ENV HOST=0.0.0.0

# Outils utiles (curl) + nettoyage
RUN apt-get update -y \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

# On copie le bundle standalone généré par Next
# .next/standalone contient server.js et node_modules nécessaires
COPY --from=builder /app/.next/standalone ./
# assets statiques
COPY --from=builder /app/.next/static ./.next/static
COPY --from=builder /app/public ./public

# Healthcheck interne du conteneur
HEALTHCHECK --interval=20s --timeout=3s --start-period=10s --retries=3 \
  CMD curl -fsS http://127.0.0.1:${PORT}/health || exit 1

EXPOSE 3000
# Lancement du serveur Next standalone
CMD ["node", "server.js"]
