# ====== Étape deps ======
FROM node:20-bullseye-slim AS deps
WORKDIR /app

# Copie des manifestes (le * évite d'échouer s'il n'y a pas de lock)
COPY package.json package-lock.json* ./

# Install: si lock => npm ci, sinon => npm install
RUN set -eux; \
    if [ -f package-lock.json ]; then \
      npm ci; \
    else \
      npm install --no-audit --no-fund; \
    fi

# ====== Étape build ======
FROM node:20-bullseye-slim AS builder
WORKDIR /app
ENV NEXT_TELEMETRY_DISABLED=1

COPY --from=deps /app/node_modules ./node_modules
COPY . .

# Build Next en mode standalone
RUN npm run build

# ====== Étape run ======
FROM node:20-bullseye-slim AS runner
ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
ENV PORT=3000
ENV HOST=0.0.0.0

# Outils utiles (diag/health)
RUN apt-get update -y \
 && apt-get install -y --no-install-recommends curl procps iproute2 \
 && rm -rf /var/lib/apt/lists/*

# On place le bundle standalone dans /app/standalone
WORKDIR /app
COPY --from=builder /app/.next/standalone /app/standalone
# IMPORTANT : on met les assets là où server.js s'attend à les trouver
COPY --from=builder /app/.next/static     /app/standalone/.next/static
COPY --from=builder /app/public           /app/standalone/public

# Sanity checks
RUN test -f /app/standalone/server.js && ls -la /app/standalone | head -n 50

# Healthcheck interne
HEALTHCHECK --interval=20s --timeout=3s --start-period=10s --retries=3 \
  CMD curl -fsS http://127.0.0.1:${PORT}/health || exit 1

EXPOSE 3000

# On lance depuis le DOSSIER standalone (chemins relatifs corrects)
WORKDIR /app/standalone
CMD ["node", "server.js"]
