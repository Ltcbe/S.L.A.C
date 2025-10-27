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
 && apt-get install -y --no-install-recommends curl iproute2 procps libc-bin \
 && rm -rf /var/lib/apt/lists/*

# Bundle standalone + assets
WORKDIR /app
COPY --from=builder /app/.next/standalone /app/standalone
COPY --from=builder /app/.next/static     /app/standalone/.next/static
COPY --from=builder /app/public           /app/standalone/public

# Sanity check
RUN test -f /app/standalone/server.js

# Healthcheck: cible l'IP du conteneur (certaines builds n'écoutent pas sur 127.0.0.1)
HEALTHCHECK --interval=20s --timeout=3s --start-period=10s --retries=3 \
  CMD IP=$(hostname -i | awk '{print $1}'); curl -fsS "http://${IP}:${PORT}/health" || exit 1

EXPOSE 3000

# Lancement depuis le dossier standalone
WORKDIR /app/standalone
CMD ["node", "server.js"]
