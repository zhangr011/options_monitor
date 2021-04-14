#!/bin/bash

SERVICE_NAME=options_monitor
PID=$SERVICE_NAME.pid
ProcNumber=`ps -ef | grep -w $SERVICE_NAME | grep -v grep | wc -l`

pyimm=
imm=
pypush=
push=
pyrecalc_siv=
recalc_siv=
pyscp=
scp=

while getopts "m:iprc" opt; do
    case ${opt} in
        m)
            mode=$OPTARG
            ;;
        i)
            pyimm='--imm=True'
            imm='-i'
            ;;
        p)
            pypush='--push=True'
            push='-p'
            ;;
        r)
            pyrecalc_siv='--recalculate_siv=True'
            recalc_siv='-r'
            ;;
        c)
            pyscp='--scp=True'
            scp='-c'
            ;;
        *)
            echo 'unknown argument. '
    esac
done

case "$mode" in
    start)
        if [ -f ./$PID ]; then
            echo "$SERVICE_NAME is started, please use the restart option. "
        else
            nohup python3 ./options_monitor.py $pyimm $pypush $pyrecalc_siv $pyscp 2>&1 &
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
        $0 -m stop
        sleep 2
        $0 -m start $imm $push $recalc_siv $scp
        ;;
    *)
        echo "Usage: bash start_monitor.sh -m [start|stop|restart]"
        echo `python3 ./options_monitor.py --help`
        ;;
esac
exit 0
