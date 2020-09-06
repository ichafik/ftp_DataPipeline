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

# -------
# If the following error occurs: "ls: cannot access /home/datalab/spark-2.3.1-bin-hadoop2.7/lib/spark-assembly-*.jar: No such file or directory"
# Then edit the following file: "/opt/cloudera/parcels/CDH/lib/hive/bin/hive"
# And comment the following lines as below:
#"""
#if [[ -n "$SPARK_HOME" && !("$HIVE_SKIP_SPARK_ASSEMBLY" = "true") ]]
#then
#  sparkAssemblyPath=`ls ${SPARK_HOME}/lib/spark-assembly-*.jar`
#  CLASSPATH="${CLASSPATH}:${sparkAssemblyPath}"
#fi
#"""
# For more information please check the following link: https://stackoverflow.com/questions/39254865/spark-installation-spark-2-0-0-bin-hadoop2-7-lib-spark-assembly-jar-no-such
# -------
# If the following warning occurs: "Java HotSpot(TM) 64-Bit Server VM warning: ignoring option MaxPermSize=512M; support was removed in 8.0"
# Then edit the following file: "/opt/cloudera/parcels/CDH/lib/hive/bin/hive"
# And add the line HADOOP_CLIENT_OPTS=... just after fi:
#$ sudo vim /opt/cloudera/parcels/CDH/lib/hive/bin/hive
#if [ -f "${HIVE_CONF_DIR}/hive-env.sh" ]; then
#  . "${HIVE_CONF_DIR}/hive-env.sh"
#fi
#HADOOP_CLIENT_OPTS=`echo $HADOOP_CLIENT_OPTS | sed -e "s/-XX:MaxPermSize=512M //g"`
# -------

# Calling hive
CMD="-n ${HIVE_USER} -p ${HIVE_PASSWORD} -u ${HIVE_URL}"
echo $CMD
beeline $CMD "$@" 
#beeline -n "${HIVE_USER}" -p "${HIVE_PASSWORD}" -u "${HIVE_URL}" "$@"

