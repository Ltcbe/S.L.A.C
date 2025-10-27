# ========= Étape deps =========
FROM node:20-bullseye-slim AS deps
ENV NODE_ENV=production
WORKDIR /app

# On copie les manifestes depuis le dossier frontend/ (contexte = racine du repo)
COPY frontend/package.json frontend/package-lock.json* ./

# Install prod : si lock => npm ci, sinon npm install
RUN if [ -f package-lock.json ]; then npm ci --omit=dev; else npm install --omit=dev; fi

# ========= Étape builder =========
FROM node:20-bullseye-slim AS builder
ENV NODE_ENV=production
WORKDIR /app

# Node modules
COPY --from=deps /app/node_modules ./node_modules
# Code source du frontend
COPY frontend/. .

# Build Next (exige output:'standalone' dans next.config.js)
RUN npm run build

# ========= Étape runner =========
FROM node:20-bullseye-slim AS runner
ENV NODE_ENV=production
ENV PORT=3000
ENV HOST=0.0.0.0
ENV NEXT_TELEMETRY_DISABLED=1
WORKDIR /app

# curl pour healthcheck
RUN apt-get update -y \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

# Bundle standalone
COPY --from=builder /app/.next/standalone ./
# Assets statiques
COPY --from=builder /app/.next/static ./.next/static
# Public
COPY --from=builder /app/public ./public

EXPOSE 3000
CMD ["node", "server.js"]
