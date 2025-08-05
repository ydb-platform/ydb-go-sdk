#!/bin/sh -e

get_random_container() {
    # Get a list of all containers starting with ydb-database-*
    containers=$(docker ps --format '{{.Names}}' | grep '^ydb-database-')

    # Convert the list to a newline-separated string
    containers=$(echo "$containers" | tr ' ' '\n')

    # Count the number of containers
    containersCount=$(echo "$containers" | wc -l)

    # Generate a random number between 0 and containersCount - 1
    randomIndex=$(shuf -i 0-$(($containersCount - 1)) -n 1)

    # Get the container name at the random index
    nodeForChaos=$(echo "$containers" | sed -n "$(($randomIndex + 1))p")
}

get_random_container
sh -c "docker stop ${nodeForChaos} -t 30"
sh -c "docker start ${nodeForChaos}"

sleep 60

get_random_container
sh -c "docker restart ${nodeForChaos} -t 0"

sleep 60

get_random_container
sh -c "docker kill -s SIGKILL ${nodeForChaos}"

sleep 60