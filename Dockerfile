FROM python:3.11-slim

# Install Node.js + Salesforce CLI
RUN apt-get update && apt-get install -y curl && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    npm install -g @salesforce/cli && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5001

# Authenticate SF CLI from env var, then start the app
CMD sf org login sfdx-url --sfdx-url-stdin --set-default <<< "$SFDX_AUTH_URL" && python3 app.py
