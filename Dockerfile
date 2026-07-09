FROM python:3.11-slim

# ── Node 22 LTS (maintained until April 2027) ────────────────────────────────
# The Salesforce CLI's jsforce dependency uses undici 8.x, which requires
# Node >= 22. On Node 20 (EOL April 2026) the CLI crashes at load with
# "TypeError: webidl.util.markAsUncloneable is not a function" — the root
# cause of the 9 Jul 2026 production outage (verified by local reproduction).
RUN apt-get update && apt-get install -y curl gnupg && \
    curl -fsSL https://deb.nodesource.com/setup_22.x | bash - && \
    apt-get install -y nodejs && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# ── Salesforce CLI — PINNED for reproducible builds; bump deliberately ──────
# (verified working on Node 22.23.1 on 9 Jul 2026)
RUN npm install -g @salesforce/cli@2.144.0

# ── Build-time smoke test ────────────────────────────────────────────────────
# Exercise the same heavy jsforce/undici code path that start.sh's
# `sf org login sfdx-url` hits at container startup. With a dummy auth URL a
# HEALTHY CLI answers INVALID_SFDX_AUTH_URL; a broken CLI throws before it can
# validate anything. If this fails, the BUILD fails — and Railway keeps the
# previous deployment running instead of shipping a crash-looping container.
RUN sf --version && \
    echo dummy > /tmp/smoke_auth.txt && \
    (sf org login sfdx-url --sfdx-url-file /tmp/smoke_auth.txt --no-prompt 2>&1 || true) | tee /tmp/smoke.log && \
    rm -f /tmp/smoke_auth.txt && \
    grep -q "INVALID_SFDX_AUTH_URL" /tmp/smoke.log && rm /tmp/smoke.log

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# /data will be mounted as Railway persistent volume for campaigns.json
RUN mkdir -p /data

EXPOSE 5001

# app.py handles SF JWT auth on startup via setup_sf_auth()
CMD ["bash", "start.sh"]
