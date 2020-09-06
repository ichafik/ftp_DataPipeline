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

# The job file name
JOB_FILE_NAME=$1

# Create a temp directory for Spark warehouse
#SPARK_WAREHOUSE_DIR=`mktemp -d`

#export _PYSPARK_DRIVER_CONN_INFO_PATH="${SPARK_WAREHOUSE_DIR}/conn"

# List of jdbc libraries
if [ -d "${APPLICATION_HOME}/lib/jdbc" ]; then
    JDBC_LIST=$(JARS=(${APPLICATION_HOME}/lib/jdbc/*.jar); IFS=,; echo "${JARS[*]}")
fi

# List of jars libraries
if [ -d "${APPLICATION_HOME}/lib/jar" ]; then
    export JAR_LIST=$(JARS=(${APPLICATION_HOME}/lib/jar/*.jar); IFS=,; echo "${JARS[*]}")
    if [ ! "${JDBC_LIST}" = "" ]; then
        JAR_LIST="${JAR_LIST},${JDBC_LIST}"
    fi
fi

# Default jdbc librairies
if [ "${JDBC_LIST}" = "" ]; then
    JDBC_LIST="${APPLICATION_HOME}/lib/jdbc/*.jar"
fi

# Default jar librairies
if [ "${JAR_LIST}" = "" ]; then
    JAR_LIST="${APPLICATION_HOME}/lib/jar/*.jar"
fi

# Show the list of jar libraries
echo "JAR_LIST:"
echo "--jars ${JAR_LIST}"
echo

# Show the list of jdbc libraries
echo "JDBC_LIST:"
echo "--driver-class-path ${JDBC_LIST}"
echo

# Execute the Spark job
${SPARK_HOME}/bin/spark-submit \
	--master "yarn" \
	--deploy-mode "client" \
	--name "${APPLICATION_NAME}" \
	--conf "spark.driver.memory=${SPARK_DRIVER_MEMORY}" \
	--conf "spark.driver.maxResultSize=${SPARK_DRIVER_MAXRESULTSIZE}" \
	--conf "spark.yarn.executor.memoryOverhead=${SPARK_YARN_EXECUTOR_MEMORYOVERHEAD}" \
	--conf "spark.eventLog.enabled=true" \
	--conf "spark.eventLog.dir=hdfs:///spark2-history" \
	--conf "spark.ui.port=${SPARK_MASTER_WEBUI_PORT}" \
	--files "/etc/hive/conf.cloudera.hive/hive-site.xml" \
	--jars ${JAR_LIST} \
	--driver-class-path ${JDBC_LIST} \
    ${JOB_FILE_NAME}

#	--packages com.databricks:spark-xml_2.11:0.4.1,com.stratio.datasource:spark-mongodb_2.11:0.12.0 \

# Delete the temp directory of Spark warehouse
#--conf "spark.sql.warehouse.dir=${SPARK_WAREHOUSE_DIR}" \
#rm -rf ${SPARK_WAREHOUSE_DIR}

