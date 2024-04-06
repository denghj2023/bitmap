#!/bin/sh

# Define the JAR file name
JAR_FILE="bitmap.jar"

# Function to get the PID of the JAR process
get_pid() {
    jps -l | grep "$JAR_FILE" | awk '{print $1}'
}

# Function to terminate the process
terminate_process() {
    local pid=$1
    if [ -n "$pid" ]; then  # Check if PID is non-empty
        echo "Attempting to kill process $pid ..."
        kill "$pid"  # Attempt to kill the process gracefully
        local attempts=0
        while [ "$attempts" -lt 10 ]; do  # Wait up to 10 seconds for the process to terminate
            if [ -n "$(get_pid)" ]; then
                echo "Waiting for process $pid to terminate... ${attempts}s"
                sleep 1  # Wait for 1 second before checking again
            else
                echo "Process $pid has been terminated gracefully."
                return 0
            fi
            attempts=$((attempts + 1))
        done
        echo "Process $pid did not terminate after 10 seconds. Forcing shutdown..."
        kill -9 "$pid"  # Force kill the process if it didn't terminate gracefully
        if [ -z "$(get_pid)" ]; then
            echo "Process forcefully terminated."
        else
            echo "Failed to terminate process $pid."
            return 1
        fi
    else
        echo "No process running for $JAR_FILE."
    fi
}

# Main script execution starts here
PID=$(get_pid)  # Get the PID of the running JAR process
terminate_process "$PID"  # Attempt to terminate the process
