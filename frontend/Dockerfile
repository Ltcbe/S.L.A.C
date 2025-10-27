# ====== Étape deps ======
FROM node:20-bullseye-slim AS deps
WORKDIR /app

# Copie des manifestes (le * n'échoue pas s'il n'y a pas de lock)
COPY frontend/package.json frontend/package-lock.json* ./

# Install: si lock => npm ci, sinon => npm install
RUN if [ -f package-lock.json ]; then npm ci; else npm install; fi

# ====== Étape build ======
FROM node:20-bullseye-slim AS builder
WORKDIR /app

COPY --from=deps /app/node_modules ./node_modules
COPY frontend/ ./

# Build en mode standalone (assure-toi que next.config.js a `output: 'standalone'`)
RUN npm run build

# ====== Étape run ======
FROM node:20-bullseye-slim AS runner
WORKDIR /app
ENV NODE_ENV=production
ENV HOST=0.0.0.0
ENV PORT=3000

# utilitaires pour healthcheck
RUN apt-get update -y \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

# Copie du bundle standalone
COPY --from=builder /app/.next/standalone ./standalone
COPY --from=builder /app/public ./standalone/public
COPY --from=builder /app/.next/static ./standalone/.next/static

EXPOSE 3000
HEALTHCHECK --interval=10s --timeout=5s --retries=30 CMD curl -fsS http://127.0.0.1:3000/health || exit 1

CMD ["node", "/app/standalone/server.js"]
