FROM node:24-alpine

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY relay.mjs ./

ENV NODE_ENV=production
# Outbound/DNS consistency with the other NUC services.
ENV NODE_OPTIONS="--dns-result-order=ipv4first --network-family-autoselection-attempt-timeout=2000"
ENV PORT=8080

RUN chown -R node:node /app
USER node

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -q -T 4 -O - "http://127.0.0.1:${PORT:-8080}/" | grep -q 'moblin-relay ok'

CMD ["node", "relay.mjs"]
