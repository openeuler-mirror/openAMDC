
case "$1" in
    start)
        if [ -f $PIDFILE ]
        then
            echo "$PIDFILE exists, process is already running or crashed"
        else
            echo "Starting openAMDC server..."
            $EXEC $CONF
        fi
        ;;
    stop)
        if [ ! -f $PIDFILE ]
        then
            echo "$PIDFILE does not exist, process is not running"
        else
            PID=$(cat $PIDFILE)
            echo "Stopping ..."
            $CLIEXEC -p $OPENAMDCPORT shutdown
            while [ -x /proc/${PID} ]
            do
                echo "Waiting for openAMDC to shutdown ..."
                sleep 1
            done
            echo "openAMDC stopped"
        fi
        ;;
    status)
        PID=$(cat $PIDFILE)
        if [ ! -x /proc/${PID} ]
        then
            echo 'openAMDC is not running'
        else
            echo "openAMDC is running ($PID)"
        fi
        ;;
    restart)
        $0 stop
        $0 start
        ;;
    *)
        echo "Please use start, stop, restart or status as first argument"
        ;;
esac
