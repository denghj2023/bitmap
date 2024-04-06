#!/bin/sh

#===========================================================================================
# Application Configuration
#===========================================================================================
HOME_DIR=$(cd "$(dirname $0)";pwd)
JAR_FILE=${HOME_DIR}/target/bitmap.jar
LOG_HOME=${HOME_DIR}/logs
APP_OPT="-Dlog.home=${LOG_HOME} -Dserver.port=8080 -Dlogging.config=classpath:logback-server.xml -Dspring.elasticsearch.uris=http://10.0.0.10:9200"
PID_FILE=${HOME_DIR}/pid # Corrected PID_FILE assignment

#===========================================================================================
# JVM Configuration
#===========================================================================================
JAVA_OPT="${JAVA_OPT} -server -Xms512m -Xmx512m"
JAVA_OPT="${JAVA_OPT} -verbose:gc -Xloggc:${LOG_HOME}/gc_%p.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps"
JAVA_OPT="${JAVA_OPT} -XX:+PrintGCApplicationStoppedTime -XX:+PrintAdaptiveSizePolicy"
JAVA_OPT="${JAVA_OPT} -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=30m"

# Check if LOG_HOME exists, if not create it
if test ! -d "$LOG_HOME"; then
  mkdir -p "$LOG_HOME"
fi

# Check if the application is already running
if [ -f "$PID_FILE" ]; then
  if ps -p $(cat "$PID_FILE") > /dev/null 2>&1; then
    echo "The application is already running."
    exit 1
  else
    # Process not found, remove the stale PID file
    echo "Stale PID file found. Removing."
    rm -f "$PID_FILE"
  fi
fi

# Start the application
nohup java ${JAVA_OPT} ${APP_OPT} -jar $JAR_FILE > ${LOG_HOME}/nohup.out 2>&1 &

# Get the process ID of the last background command
echo $! > "$PID_FILE"
