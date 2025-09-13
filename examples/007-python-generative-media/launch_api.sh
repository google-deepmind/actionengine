#!/bin/bash
while true; do
    date
    TURN_SERVER_SHARED_SECRET=2cbe5a33a5fda257747d75863bd9ccb8920b9249 fastapi run httpapi.py
    echo "[$(date)] Restarting after crash..."
    sleep 2
done