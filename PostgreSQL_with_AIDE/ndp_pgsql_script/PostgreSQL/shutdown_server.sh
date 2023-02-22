#!/bin/bash

# Change to this-file-exist-path.
IROOTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"/ 
cd $IROOTDIR

cd $IROOTDIR
./script/script_server/shutdown_server.sh
