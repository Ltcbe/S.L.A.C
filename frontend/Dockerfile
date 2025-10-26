# --- frontend/Dockerfile (Debian slim, sans Alpine/musl) ---
FROM node:20-bullseye-slim AS deps
WORKDIR /app
COPY package.json package-lock.json* ./
# Utilise npm ci si un lockfile existe ; sinon fallback npm install
RUN if [ -f package-lock.json ]; then npm ci; else npm install --no-audit --fund=false --progress=false; fi

FROM node:20-bullseye-slim AS builder
WORKDIR /app
ENV NEXT_TELEMETRY_DISABLED=1
# (optionnel) si le build manque de RAM sur ton runner Dokploy, décommente la ligne ci-dessous :
# ENV NODE_OPTIONS="--max-old-space-size=512"
COPY --from=deps /app/node_modules ./node_modules
COPY . .
RUN npm run build

FROM node:20-bullseye-slim AS runner
WORKDIR /app
ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
COPY --from=builder /app/.next ./.next
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/public ./public
COPY --from=builder /app/package.json ./package.json
EXPOSE 3000
CMD ["npm", "start"]
