#!/bin/bash
set -e
DIR=$(pwd)
export PYTHONPATH=$(pwd)

run_consumer() {
    CONSUMER_TYPE="$1"
    shift              # Remove the first argument
    CONSUMER_ARGS="$@" # All remaining arguments

    CONSUMER_SCRIPT="src/consumers/${CONSUMER_TYPE}/consumer.py"

    if [ -f "$CONSUMER_SCRIPT" ]; then
        echo "Starting the ${CONSUMER_TYPE} consumer with arguments: ${CONSUMER_ARGS}"
        python "$CONSUMER_SCRIPT" $CONSUMER_ARGS
    else
        echo "Error: Consumer script not found for type '${CONSUMER_TYPE}'"
        exit 1
    fi
}

case "$1" in
"producer")
    echo "Starting the uvicorn server"
    uvicorn src.main:app --workers 1 --host 0.0.0.0 --port 8000 --lifespan=on
    ;;
"consumer")
    if [ -z "$2" ]; then
        echo "Error: Consumer type not specified. Usage: consumer <consumer_type> [additional_args...]"
        exit 1
    fi
    run_consumer "${@:2}"
    ;;
*)
    echo "Invalid command. Use 'producer' or 'consumer'."
    exit 1
    ;;
esac

if [ $? -ne 0 ]; then
    echo "[ERROR] Command failed"
    exit 1
else
    echo "Command executed successfully"
fi
