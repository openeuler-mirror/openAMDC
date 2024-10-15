#!/bin/bash

HOST=localhost
PORT=6379
CLIENTS=50
REQUESTS=5000
MAXTHREADS=$(nproc)

# Process option parameters
while getopts ":hs:p:c:n:t:" opt; do
  case $opt in
    h)
        echo "Usage: find-best-worker-threads.sh [-s <hostname>] [-p <port>] [-c <clients>] [-n <requests>] [-t <max threads>]"
        echo " -s <hostname>      Server hostname (default 127.0.0.1)\n"
        echo " -p <port>          Server port (default 6379)"
        echo " -c <clients>       Number of parallel connections (default 50)"
        echo " -n <requests>      Total number of requests (default 5000)"
        echo " -t <max threads>   Max number of threads used during testing (default cpu cores)"
        exit 1
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


# Compile openamdc
ROOT=$PWD/../
make -C $ROOT -j$(nproc)


# Check if memtier_benchmark is installed
benchmark="memtier_benchmark"
if which $benchmark >/dev/null; then
    echo "$benchmark is installed."
else
    echo "$benchmark is not installed."
    exit 1
fi


# Loop through different number of worker threads
THREAD=1
while [ $THREAD -le $MAXTHREADS ]; do
    # Starting openamdc
    ../src/openamdc-server --bind $HOST --port $PORT --worker-threads $THREAD > /dev/null 2> /dev/null &
    
    # Waiting for openamdc server startup
    sleep 1

    pid=$(pgrep openamdc-server)
    if [ -z "$pid" ]; then
        echo "Failed to start openamdc server. Exiting."
        exit 1
    else
        echo "OpenAMDC server is already running, ../src/openamdc-server --port $PORT --worker-threads $THREAD > /dev/null 2> /dev/null &."
    fi

    echo "Starting benchmark, $benchmark -s $HOST -p $PORT -t $THREAD -c $CLIENTS -n $REQUESTS --hide-histogram --distinct-client-seed --command="set __key__ __data__" --key-prefix="kv_" --key-minimum=1 --key-maximum=10000 -R -d 128"

    # Execute memtier_benchmark
    result=$($benchmark -s $HOST -p $PORT -t $THREAD -c $CLIENTS -n $REQUESTS --hide-histogram --distinct-client-seed --command="set __key__ __data__" --key-prefix="kv_" --key-minimum=1 --key-maximum=10000 -R -d 128)
    echo -e "$result\n\n\n\n\n"
    
    # Stop the service
    kill -9 $pid
    
    ((THREAD++))
done
