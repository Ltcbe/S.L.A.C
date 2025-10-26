# --- frontend/Dockerfile ---
FROM node:20-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json* ./
# Utilise npm ci si un lockfile existe, sinon fallback vers npm install
RUN if [ -f package-lock.json ]; then npm ci; else npm install --no-audit --fund=false --progress=false; fi

FROM node:20-alpine AS builder
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY . .
RUN npm run build

FROM node:20-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production
COPY --from=builder /app/.next ./.next
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/public ./public
COPY --from=builder /app/package.json ./package.json
EXPOSE 3000
CMD ["npm", "start"]
