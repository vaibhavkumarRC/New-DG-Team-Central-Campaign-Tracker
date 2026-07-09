FROM python:3.11-slim

# Install Node.js 20 (required for Salesforce CLI)
RUN apt-get update && apt-get install -y curl gnupg && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Salesforce CLI globally.
# Pinned to a specific version so builds are reproducible AND to bust any stale
# Docker cache layer. A stale cached layer had @jsforce/jsforce-node pulling the
# broken undici 8.0.3 (TypeError: webidl.util.markAsUncloneable is not a function),
# which crashed the sf CLI at startup. A clean install of this version resolves
# jsforce-node 3.10.x -> undici 8.5.0 (past the 8.0.3 regression). Bump deliberately.
RUN npm install -g @salesforce/cli@2.144.0

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# /data will be mounted as Railway persistent volume for campaigns.json
RUN mkdir -p /data

EXPOSE 5001

# app.py handles SF JWT auth on startup via setup_sf_auth()
CMD ["bash", "start.sh"]
