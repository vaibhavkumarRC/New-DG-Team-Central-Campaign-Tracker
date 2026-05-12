#!/bin/bash
set -e

echo "=== Authenticating Salesforce CLI ==="
printf '%s' "$SFDX_AUTH_URL" > /tmp/sfdx_auth_url.txt
sf org login sfdx-url --sfdx-url-file /tmp/sfdx_auth_url.txt --set-default --no-prompt
rm -f /tmp/sfdx_auth_url.txt

echo "=== Starting Flask App ==="
exec python3 app.py
