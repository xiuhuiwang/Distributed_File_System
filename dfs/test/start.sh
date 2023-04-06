#!/usr/bin/env bash

port_prefix=28 # Put your assigned port prefix here.
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
    port=$(( port_prefix * 1000 + i + 520 ))
    server=$(( i % ${#servers[@]} ))

    # This will ssh to the machine, and run 'node orion01 <some port>' in the
    # background.
    echo "Starting node on ${servers[${server}]} on port ${port}"
    ssh ${servers[${server}]} "${HOME}/CS667-BigData/P1-wonderpea/dfs/node/node orion02:28999 ${port}" &
done

echo "Startup complete"
