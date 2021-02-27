#!/bin/bash

SERVICE_NAME=proxyPool
SCHEDULE_PID=$SERVICE_NAME.schedule.pid
SERVER_PID=$SERVICE_NAME.server.pid

cd ./3rd/proxy_pool.git

while getopts "m:" opt; do
    case ${opt} in
        m)
            mode=$OPTARG
            ;;
        *)
            echo 'unknown argument. '
    esac
done

case "$mode" in
    start)
        if [ -f ./$SERVER_PID ]; then
            echo "$SERVER_PID is started, please use the restart option. "
        else
            nohup python3 ./proxyPool.py server &
            echo $! > ./$SERVER_PID
            echo "==== start $SERVER_PID ===="
        fi
        if [ -f ./$SCHEDULE_PID ]; then
            echo "$SCHEDULE_PID is started, please use the restart option. "
        else
            nohup python3 ./proxyPool.py schedule &
            echo $! > ./$SCHEDULE_PID
            echo "==== start $SCHEDULE_PID ===="
        fi
        ;;
    stop)
        kill -9 `cat ./$SCHEDULE_PID`
        rm -rf ./$SCHEDULE_PID
        sleep 1
        kill -9 `cat ./$SERVER_PID`
        rm -rf ./$SERVER_PID
        echo "==== stop $SERVICE_NAME ===="
        ;;
    restart)
        $0 -m stop
        sleep 2
        $0 -m start
        ;;
    *)
        echo "Usage: bash start_monitor.sh [start|stop|restart]"
        ;;
esac
exit 0
