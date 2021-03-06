
prog=king
jar=/opt/king/king.jar

OPTS=""
JOPTS=""

if [ -f /etc/king/setenv.sh ]; then
    . /etc/king/setenv.sh
fi

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

checkJava
checkJar
$java -jar $JOPTS $jar $OPTS 

