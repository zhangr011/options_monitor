#!/bin/bash

SERVICE_NAME=flask_monitor
PID=$SERVICE_NAME.pid

case "$1" in
    start)
        if [ -f ./$PID ]; then
            echo "$SERVICE_NAME is started, please use the restart option. "
        else
            nohup python3 ./pyecharts_flask_kline/server.py > flask.out 2>&1 &
            echo $! > ./$PID
            echo "==== start $SERVICE_NAME ===="
        fi
        ;;
    stop)
        kill -9 `cat ./$PID`
        rm -rf ./$PID
        echo "==== stop $SERVICE_NAME ===="
        ;;
    restart)
        $0 stop
        sleep 2
        $0 start
        ;;
    *)
        echo "Usage: bash start_flask.sh [start|stop|restart]"
        ;;
esac
exit 0
