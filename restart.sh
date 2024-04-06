#!/bin/sh

DIR=$(cd "$(dirname $0)";pwd)

echo "Stop ..."
sh $DIR/stop.sh

echo "Start ..."
sh $DIR/startup.sh