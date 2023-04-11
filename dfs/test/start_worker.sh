#!/usr/bin/env bash

port_prefix=26 # Put your assigned port prefix here.
               # See: https://www.cs.usfca.edu/~mmalensek/cs677/schedule/materials/ports.html
nodes=12      # Number of nodes to run

# Server list. You can comment out servers that you don't want to use with '#'
servers=(
    "orion01"
    "orion02"
    "orion03"
    "orion04"
    "orion05"
    "orion06"
    "orion07"
    "orion08"
    "orion09"
    "orion10"
    "orion11"
    "orion12"
)

for (( i = 0; i < nodes; i++ )); do
    port=$(( port_prefix * 1000 + i + 690 ))
    server=$(( i % ${#servers[@]} ))

    # This will ssh to the machine, and run 'node orion01 <some port>' in the
    # background.
    echo "Starting worker on ${servers[${server}]} on port ${port}"
    ssh ${servers[${server}]} "${HOME}/big_data/P2-map_increase/dfs/MRWorker/worker orion02:26997 ${port}" &
done

echo "Startup complete"
