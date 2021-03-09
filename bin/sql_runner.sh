#!/usr/bin/env bash

BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
export JAVA_HOME=/usr/java/latest

if [ ! $# -ge 1 ]; then
  echo "job config file must be provided!"
  exit 1
fi

jobFile=$1
if [ ! -f ${jobFile} ];then
  jobFile="${BASEDIR}/${jobFile}"
  if [ ! -f ${jobFile} ];then
    echo "没有找到job文件: "${jobFile}
    exit
  fi
fi
jobFile=$(readlink -f ${jobFile})
shift

cd ${BASEDIR}

export SPARK_HOME=/opt/spark
export CLASSPATH=$(echo ${SPARK_HOME}/jars/*.jar | tr ' ' ':'):${CLASSPATH}
export CLASSPATH=$(echo ${BASEDIR}/lib/*.jar | tr ' ' ':'):${CLASSPATH}

export HADOOP_CONF_DIR=/etc/hadoop/conf
export SPARK_CONF_DIR=${SPARK_HOME}/conf
export CLASSPATH=${HADOOP_CONF_DIR}:${SPARK_CONF_DIR}:${CLASSPATH}

export JAVA_OPTS="-Xmx2048m -Xms256m -server -XX:+UseG1GC"
jobFileName=$(basename ${jobFile})

export CLASSPATH=${BASEDIR}/conf:${CLASSPATH}
stdoutFile="/tmp/$USER/$(date +%Y%m%d)/${jobFileName%%.*}_$(date +%Y%m%d_%H%M%S).stdout"
mkdir -p "/tmp/$USER/$(date +%Y%m%d)"
if [[ "$*" =~ "--test" ]];then
  export JAVA_OPTS="${JAVA_OPTS} -Dinsight.root.logger=INFO,CA,FA -Dinsight.file.stdout=${stdoutFile}"
elif [[ "$*" =~ "--dryrun" ]]; then
  export JAVA_OPTS="${JAVA_OPTS} -Dinsight.root.logger=INFO,CA,FA -Dinsight.file.stdout=${stdoutFile}"
else
  export JAVA_OPTS="${JAVA_OPTS} -Dinsight.root.logger=INFO,FA -Dinsight.file.stdout=${stdoutFile}"
fi

# echo "Using CLASSPATH:"$CLASSPATH
export HADOOP_USER_NAME=schedule
if [[ "$*" =~ "--test" ]];then
  "${JAVA_HOME}"/bin/java ${JAVA_OPTS} org.apache.sql.runner.JobRunner ${jobFile} $@
elif [[ "$*" =~ "--dryrun" ]]; then
  "${JAVA_HOME}"/bin/java ${JAVA_OPTS} org.apache.sql.runner.JobRunner ${jobFile} $@
else
  "${JAVA_HOME}"/bin/java ${JAVA_OPTS} org.apache.sql.runner.JobRunner ${jobFile} $@ 2>>"${stdoutFile}"
fi
if [ $? -ne 0 ];then
  "${JAVA_HOME}"/bin/java org.apache.sql.runner.Alert ${jobFile} $@
fi
