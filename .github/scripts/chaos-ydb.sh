#!/bin/bash

sleep 30

chaos_ydb_dynamic_containers() {
    # Set the end time to 3 minutes (180 seconds) from the start
    end_time=$((SECONDS + 180))
    pattern="ydb-dynamic"

    while [ $SECONDS -lt $end_time ]; do
        signal=${1:-"SIGTERM"}
        time=${2:-"5"}

        containers=$(docker ps --filter "name=$pattern" -q)

        for container_id in $containers; do
            echo "Restarting container with signal $signal: $container_id"
            docker restart --signal $signal --time $time "$container_id"

            sleep 30
        done
    done
}

chaos_ydb_dynamic_containers "SIGTERM";

chaos_ydb_dynamic_containers "SIGINT";

chaos_ydb_dynamic_containers "SIGKILL" 0;
