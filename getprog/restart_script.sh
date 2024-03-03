#!/bin/bash

script_name="scraper.py"
# Get the directory of the current script
script_dir="$(dirname "$(readlink -f "$0")")"
project_path="$script_dir"  # Set this to the directory of the script
script_path="$project_path/$script_name"

while true; do
    unique_id=$(uuidgen)  # Generate a unique identifier
    echo "Running script with unique ID: $unique_id"

    # Start a new instance of the script with the unique identifier
    (cd "$project_path" && poetry run python "$script_path" "$unique_id") &

    # Wait for a specified time before restarting the loop
    sleep 1800  # Adjust the time as needed

    # Find the process ID of the script and terminate it
    pkill -f "$unique_id"
done
