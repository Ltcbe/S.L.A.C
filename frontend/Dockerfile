# ====== Étape deps ======
FROM node:20-bullseye-slim AS deps
WORKDIR /app

# Copie des manifestes. Le * permet de ne pas échouer s'il n'y a pas de package-lock.json
COPY package.json package-lock.json* ./

# Install:
# - si package-lock.json présent => npm ci (reproductible)
# - sinon => npm install (fallback)
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

# Reprend les node_modules installés et les sources
COPY --from=deps /app/node_modules ./node_modules
COPY . .

# Build Next en mode standalone
RUN npm run build

# ====== Étape run ======
FROM node:20-bullseye-slim AS runner
WORKDIR /app

ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
ENV PORT=3000
ENV HOST=0.0.0.0

# Outils utiles pour health/diag
RUN apt-get update -y \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

# Bundle standalone + assets statiques
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static
COPY --from=builder /app/public ./public

# Healthcheck du conteneur
HEALTHCHECK --interval=20s --timeout=3s --start-period=10s --retries=3 \
  CMD curl -fsS http://127.0.0.1:${PORT}/health || exit 1

EXPOSE 3000
CMD ["node", "server.js"]
