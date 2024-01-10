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

# Start block
if [ "$1" == "start" ]; then

    echo "Starting Fire Departament JOB"
    # Install and Activate raw virtual environment
    python3 -m venv "$CURRENT_DIR/venv"
    source "$CURRENT_DIR/venv/bin/activate" && echo "Virtual environment is active."

    # Run Main Fire Department Job
    pip install -r "$CURRENT_DIR/../app/src/requirements.txt"
    python "$CURRENT_DIR/../app/src/app.py"

    # Deactivate & remove virtual environment
    deactivate && echo "Virtual environment is not active."
    rm -rf "$CURRENT_DIR/venv"

fi

