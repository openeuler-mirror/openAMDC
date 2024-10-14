#!/bin/bash

HOST=localhost
PORT=6379
CLIENTS=50
REQUESTS=5000
MAXTHREADS=$(nproc)

while getopts ":h:s:p:c:n:t:" opt; do
  case $opt in
    h)
        echo "Usage: find-best-worker-threads.sh [-h <host>] [-p <port>] [-c <clients>] [-n <requests>]"
        echo " -s <hostname>      Server hostname (default 127.0.0.1)\n"
        echo " -p <port>          Server port (default 6379)\n"
        echo " -c <clients>       Number of parallel connections (default 50)\n"
        echo " -n <requests>      Total number of requests (default 5000)\n"
        echo " -t <max threads>   Max number of threads used during testing (default cpu cores)\n"
        exit 1
        ;;
    s)
        HOST=$OPTARG
        ;;
    p)
        PORT=$OPTARG
        ;;
    c)
        CLIENTS=$OPTARG
        ;;
    n)
        REQUESTS=$OPTARG
        ;;
    t)
        if [ $OPTARG -lt $MAXTHREADS ]; then
            MAXTHREADS=$OPTARG
        fi
        ;;
    \?)
        echo "Invalid option: -$OPTARG" >&2
        exit 1
        ;;
    :)
        echo "Option -$OPTARG requires an argument." >&2
        exit 1
        ;;
  esac
done


THREAD=1
while [ $THREAD -le $MAXTHREADS ]; do
    # Starting openamdc
    ../src/openamdc-server --bind $HOST --port $PORT --worker-threads $THREAD > /dev/null 2> /dev/null &

    sleep 1  # Waiting for openamdc server startup

    pid=$(pgrep openamdc-server)
    if [ -z "$pid" ]; then
        echo "Failed to start openamdc server. Exiting."
        exit 1
    else
        echo "OpenAMDC server is already running, ../src/openamdc-server --port $PORT --worker-threads $THREAD > /dev/null 2> /dev/null &."
    fi

    echo "Starting benchmark, memtier_benchmark -s $HOST -p $PORT -t $THREAD -c $CLIENTS -n $REQUESTS --hide-histogram --distinct-client-seed --command="set __key__ __data__" --key-prefix="kv_" --key-minimum=1 --key-maximum=10000 -R -d 128"

    # Execute memtier_benchmark
    result=$(memtier_benchmark -s $HOST -p $PORT -t $THREAD -c $CLIENTS -n $REQUESTS --hide-histogram --distinct-client-seed --command="set __key__ __data__" --key-prefix="kv_" --key-minimum=1 --key-maximum=10000 -R -d 128)

    echo "Memtier_benchmark results:"
    echo "Number of worker-threads: $THREAD"
    echo -e "$result\n\n\n\n\n"

    
    # Stop the service
    kill -9 $pid
    
    ((THREAD++))
done
