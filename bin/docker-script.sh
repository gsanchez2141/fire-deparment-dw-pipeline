#!/bin/bash

# Get the directory of the script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Check if a valid argument is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 [start|stop]"
    exit 1
fi

# Change to the script's directory
cd "$SCRIPT_DIR" || exit

# Get the current directory
CURRENT_DIR="$(pwd)"

# Check the provided argument and execute the corresponding Docker Compose command
case "$1" in
    "start")

        docker-compose -f "$CURRENT_DIR/docker-compose.yml" up -d

        # Create s3 bucket, push csv file and check if it's there
        awslocal s3api create-bucket --bucket fire-department --region us-east-1
        unzip "$CURRENT_DIR/sample_data/Fire_Incidents_20240106.csv.zip" -d "$CURRENT_DIR/sample_data/"
        awslocal s3 cp "$CURRENT_DIR/sample_data/Fire_Incidents_20240106.csv" s3://fire-department/
        awslocal s3api list-objects --bucket fire-department
        rm "$CURRENT_DIR/sample_data/Fire_Incidents_20240106.csv"

        echo "Stack running with s3 sample file generated"

        ;;

    "stop")

        docker-compose -f "$CURRENT_DIR/docker-compose.yml" down -v
        ;;

    *)
        echo "Invalid argument. Usage: $0 [start|stop]"
        exit 1
        ;;
esac

exit 0
