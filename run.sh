#!/bin/bash
cd "$(dirname "$0")"
echo ""
echo "=========================================================="
echo "  🚀  Campaign Command Center"
echo "  ➜   Opening at http://localhost:5001"
echo "=========================================================="
echo ""
source venv/bin/activate
python3 app.py
