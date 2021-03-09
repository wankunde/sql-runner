#!/usr/bin/env bash

set -x

BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
export JAVA_HOME=/usr/java/latest

if [ $# == 0 ]; then
  echo "miss parameter for profile shell!"
  exit 1
fi

cmd=$1
shift
if [ "${cmd}" == "executor" ];then
  exec "${JAVA_HOME}"/bin/java -cp ./sql-runner-2.0.jar one.profiler.ProfileAgent
  exit 0
elif [ "${cmd}" == "upload" ]; then
  sleep 1s
  if [ $# == 2 -a -f $1 ]; then
    profile_file=$1
    hdfs_file=$2

    lastTime=$(stat -c %Y "$profile_file")
    now=$(date +%s)
    stop=$(( now + 120 ))
    while [ ${now} -lt ${stop} ]; do
      sleep 1s
      if [ ${lastTime} -eq $(stat -c %Y "$profile_file") ];then
        HADOOP_USER_NAME=schedule hdfs dfs -put "${profile_file}" "${hdfs_file}"
        rm "${profile_file}"
        exit 0
      fi
    done
    rm "${profile_file}"
  fi
fi
