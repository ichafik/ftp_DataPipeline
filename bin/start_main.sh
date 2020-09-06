#!/bin/bash

###############################################
# Recherche intelligente du Home du composant #
###############################################
SCRIPT_FILE=$0
SCRIPT_DIR=`(cd \`dirname ${SCRIPT_FILE}\`; pwd)`
# If the script file is a symbolic link
if [[ -L "${SCRIPT_FILE}" ]]
then
    SCRIPT_FILE=`ls -la ${SCRIPT_FILE} | cut -d">" -f2`
    SCRIPT_DIR=`(cd \`dirname ${SCRIPT_FILE}\`; pwd)`
fi
# Remove the '..' in the path (if using a link or a source)
export APPLICATION_HOME="$(dirname "${SCRIPT_DIR}")"

# --------------------------------------------------------------------------

appName=`basename $0`
appName=${appName#"start_"}
appName=${appName%".sh"}

export APPLICATION_NAME="$appName"

${APPLICATION_HOME}/bin/startJob.sh ${APPLICATION_HOME}/src/${APPLICATION_NAME}.py

