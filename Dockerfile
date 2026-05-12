FROM python:3.11-slim

# Install Node.js 20 (required for Salesforce CLI)
RUN apt-get update && apt-get install -y curl gnupg && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Salesforce CLI globally
RUN npm install -g @salesforce/cli

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# /data will be mounted as Railway persistent volume for campaigns.json
RUN mkdir -p /data

EXPOSE 5001

# app.py handles SF JWT auth on startup via setup_sf_auth()
CMD ["bash", "start.sh"]
