#!/bin/sh
#
# king <summary>
#
# chkconfig:   2345 11 88
# description: Starts and stops a single king instance on this system 
#


### BEGIN INIT INFO
# Provides: king
# Required-Start: $network 
# Required-Stop: $network 
# Default-Start: 2 3 4 5
# Default-Stop: 0 1 6
# Short-Description: This service manages the king daemon
# Description: king is a web entry point intended to push in kafka every received payload
### END INIT INFO


prog=king
jar=/opt/king/king.jar

OPTS=""
JOPTS=""

if [ -f /etc/king/setenv.sh ]; then
    . /etc/king/setenv.sh
fi


success() {
	echo "OK"
}
failure() {
	echo "PB"
}

#
# Source function library. (For success/failure centos nice display)
#
if [ -f /etc/rc.d/init.d/functions ]; then
    . /etc/rc.d/init.d/functions
fi

checkRoot() {
	if [ "$(id -u)" != "0" ]; then
   		echo "This script must be run as root" 1>&2
   		exit 1
	fi
} 

checkJavaVersion() {
	if [[ -n "$java" ]]; then
		#echo "Check $java version"
    	version=$("$java" -version 2>&1 | awk -F '"' '/version/ {print $2}')
    	#echo version "$version"
    	if [[ "$version" > "1.7" ]]; then
    	    : 
    	    #echo "version is more than 1.7. KEEP IT"
    	else         
    	    #echo "version is less than 1.7"
    	    java=""
    	fi
	fi
}

lookupJava() {

	# First test in JAVA_HOME, if defined
	if [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
	    #echo "Found java executable in JAVA_HOME"     
	    java="$JAVA_HOME/bin/java"
		checkJavaVersion
	fi

	# If not found Lookup in path
	if [[ -z "$java" ]]; then
		java=$(type -p java)	
		if [[ -n "$java" ]]; then
	    	#echo "found java executable in PATH: $java"
			checkJavaVersion
		fi
	fi

	# if not found, lookup is some typical location
	if [[ -z "$java" ]]; then
		jps=$(ls /usr/java/jdk1.[789]*/bin/java /usr/jdk64/jdk1.[789]*/bin/java 2>/dev/null)
		for jp in $jps
		do
			java=$jp
	    	#echo "found java executable in $java"
			checkJavaVersion
			if [[ -n "$java" ]]; then
				break
			fi
		done
	fi
}

checkJava() {
	lookupJava
	if [[ -n "$java" ]]; then
		:
		#echo "Will use $java"
	else 
		echo "No valid (Version >= 1.7) java found)"
		exit 1
	fi
}



checkJar() {
	if [[ ! -f $jar ]];then
		echo "Unable to find $jar"
		exit 5
	fi
}


display_status() {
	pid=`pgrep -f $jar`
	if [ $pid  ]; then
		echo "$prog is running"
	else 
		echo "$prog is NOT running"
	fi
}



start() {
	checkRoot
    checkJava
    checkJar
	pid=`pgrep -f $jar`
	if [ $pid  ]; then
		echo "$prog is already running"
	else     
	    echo -n $"Starting $prog: "
    	$java -jar $JOPTS $jar $OPTS &
    	sleep 1
    	pid=`pgrep -f $jar`
    	[ $pid ] && success || failure
    	echo
    fi
}

stop() {
	checkRoot
	pid=`pgrep -f $jar`
	if [ $pid  ]; then
	    echo -n $"Stopping $prog: "
	    kill $pid
    	sleep 1
    	pid=`pgrep -f $jar`
    	[ $pid ] && failure || success
    	echo
    else 
		echo "$prog is NOT running"
	fi
}

restart() {
    stop
    start
}

reload() {
    restart
}

force_reload() {
    restart
}


case "$1" in
    start)
        $1
        RETVAL=$?
        ;;
    stop)
        $1
        RETVAL=$?
        ;;
    restart)
        $1
        RETVAL=$?
        ;;
    reload)
        $1
        RETVAL=$?
        ;;
    force-reload)
        force_reload
        RETVAL=$?
        ;;
    status)
        pid=`pgrep -f $jar`
		if [ $pid  ]; then
			echo "$prog is running"
			RETVAL=0
		else 
			echo "$prog is NOT running"
			RETVAL=3
		fi
        ;;
    *)
        echo $"Usage: $0 {start|stop|status|restart|reload|force-reload}"
        RETVAL=2
esac
exit $RETVAL
