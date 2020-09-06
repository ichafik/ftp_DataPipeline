
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

###########################
# Chargement de la config #
###########################
. ${APPLICATION_HOME}/conf/application.conf

# --------------------------------------------------------------------------

# The target directory can be passed as a parameter
TARGET_DIR=$1
if [ "${TARGET_DIR}" = "" ]; then
    echo "The target directory can be passed as an argument !"
fi

# Convert the ipynb files to python files
CURRENT_DIR=`pwd`
TMP_DIR=`mktemp -d`
cp ${APPLICATION_HOME}/notebook/*.ipynb ${TMP_DIR}
cd ${TMP_DIR}
jupyter nbconvert --to script *.ipynb
rm *.ipynb
cd ${CURRENT_DIR}

# Copy the python files to the target direcrory (if it exists)
if [ ! "${TARGET_DIR}" = "" ]; then
    cp ${TMP_DIR}/*.py ${TARGET_DIR}
    rm -rf ${TMP_DIR}
else
    # Show the target directory
    echo "The target directory is : ${TMP_DIR}"
fi

