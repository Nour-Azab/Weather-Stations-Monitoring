#!/bin/bash

# Ensure script halts if a background execution path breaks critically
set -e

# Dynamically locate where this shell wrapper script lives on your machine
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Fire the underlying Python tracking framework
python3 "$SCRIPT_DIR/client.py" "$@"