#!/bin/bash
set -e

echo "=== Authenticating Salesforce CLI ==="
echo "$SFDX_AUTH_URL" | sf org login sfdx-url --sfdx-url-stdin --set-default --no-prompt

echo "=== Starting Flask App ==="
exec python3 app.py
