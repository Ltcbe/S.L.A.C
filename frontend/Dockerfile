# --- frontend/Dockerfile ---
FROM node:20-bullseye-slim AS deps
WORKDIR /app
COPY package.json package-lock.json* ./
# Utilise npm ci si un lockfile est présent ; sinon fallback npm install
RUN if [ -f package-lock.json ]; then npm ci; else npm install --no-audit --fund=false --progress=false; fi

FROM node:20-bullseye-slim AS builder
WORKDIR /app
ENV NEXT_TELEMETRY_DISABLED=1
# (optionnel) décommente si runner Dokploy peu-mémoire :
# ENV NODE_OPTIONS="--max-old-space-size=512"
COPY --from=deps /app/node_modules ./node_modules
COPY . .
# IMPORTANT : build en mode standalone (voir next.config.js ci-dessous)
RUN npm run build

FROM node:20-bullseye-slim AS runner
WORKDIR /app
ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
# Port interne d’écoute
ENV PORT=3001
# Copie du bundle standalone + assets
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static
COPY --from=builder /app/public ./public
# Ouvre le port interne (pas de binding hôte ici)
EXPOSE 3001
# Lance le serveur Next standalone
CMD ["node", "server.js"]
