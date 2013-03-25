#!/usr/bin/env /bin/bash

usage="Usage: $0.sh (start|stop) <job-command> <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# get arguments
startStop=$1
shift
command=$1
shift

job_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
	num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
	while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
	    num=$prev
	done
	mv "$log" "$log.$num";
    fi
}

source "${bin}/job-config.sh"

if [ "$JOB_HOME" = "" ]; then
  export JOB_HOME=/usr/lib/job
fi

if [ "$JOB_LOG_DIR" = "" ]; then
  export JOB_LOG_DIR="/var/log/job"
fi

if [ "$JOB_NICENESS" = "" ]; then
	export JOB_NICENESS=0
fi 

if [ "$JOB_PID_DIR" = "" ]; then
  JOB_PID_DIR=/var/run/job
fi

if [ "$JOB_IDENT_STRING" = "" ]; then
  export JOB_IDENT_STRING="$USER"
fi

# some variables
export JOB_LOGFILE=job-$JOB_IDENT_STRING-$command-$HOSTNAME.log
export JOB_ROOT_LOGGER="INFO,DRFA"
export WATCHDOG_ROOT_LOGGER="INFO,watchdog"
log=$JOB_LOG_DIR/job-$JOB_IDENT_STRING-$command-$HOSTNAME.out
pid=$JOB_PID_DIR/job-$JOB_IDENT_STRING-$command.pid

case $startStop in

  (start)
    mkdir -p "$JOB_PID_DIR"
    mkdir -p "$JOB_LOG_DIR"
    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    job_rotate_log $log
    echo starting $command, logging to $log
    cd "$JOB_HOME"
    nohup  nice -n ${JOB_NICENESS} "${JOB_HOME}"/bin/job $command "$@" > "$log" 2>&1 < /dev/null &
    echo $! > $pid
    sleep 1; head "$log"
    ;;
          
  (stop)

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo stopping $command
        kill `cat $pid`
      else
        echo no $command to stop
      fi
    else
      echo no $command to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac
