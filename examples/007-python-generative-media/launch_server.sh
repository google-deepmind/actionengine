#!/bin/bash
while true; do
    date
    python ./server.py
    echo "[$(date)] Restarting after crash..."
    sleep 2
done