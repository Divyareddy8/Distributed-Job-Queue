# ── Build stage ──────────────────────────────────────────────────────────────
FROM node:20-alpine AS deps
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM node:20-alpine AS runtime
WORKDIR /app

# Non-root user for security
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

COPY --from=deps /app/node_modules ./node_modules
COPY . .

# Remove dev tooling from the image
RUN rm -rf .git tests __tests__

USER appuser

EXPOSE 3000

# Default: run the API server; override with `command:` in docker-compose / k8s
CMD ["node", "src/api/server.js"]
