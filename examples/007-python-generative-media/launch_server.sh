#!/bin/bash
while true; do
    date
    # Start the server in the background
    python ./server.py &
    SERVER_PID=$!

    # Start a background timer that kills it after 5 minutes (300 seconds)
    (
        sleep 3600
        echo "[$(date)] Time limit reached — restarting server..."
        kill $SERVER_PID 2>/dev/null
    ) &

    TIMER_PID=$!

    # Wait for the server to exit (either crash or killed)
    wait $SERVER_PID

    # Kill the timer if it’s still running
    kill $TIMER_PID 2>/dev/null

    echo "[$(date)] Restarting after crash or timeout..."
    sleep 2
done