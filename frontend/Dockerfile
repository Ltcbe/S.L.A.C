# --- frontend/Dockerfile (Debian slim + Babel, écoute sur PORT=3001) ---
FROM node:20-bullseye-slim AS deps
WORKDIR /app
COPY package.json package-lock.json* ./
# npm ci si lockfile présent ; sinon fallback npm install
RUN if [ -f package-lock.json ]; then npm ci; else npm install --no-audit --fund=false --progress=false; fi

FROM node:20-bullseye-slim AS builder
WORKDIR /app
ENV NEXT_TELEMETRY_DISABLED=1
# Si le runner est limité en RAM, décommente :
# ENV NODE_OPTIONS="--max-old-space-size=512"
COPY --from=deps /app/node_modules ./node_modules
COPY . .
RUN npm run build

FROM node:20-bullseye-slim AS runner
WORKDIR /app
ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
# 👉 Change le port d’écoute interne à 3001 pour éviter tout conflit
ENV PORT=3001
COPY --from=builder /app/.next ./.next
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/public ./public
COPY --from=builder /app/package.json ./package.json
# Expose le port interne 3001 (pas de binding hôte ici)
EXPOSE 3001
# Démarre Next sur le port $PORT
CMD ["sh", "-lc", "npm start -p ${PORT}"]
