# Trouble Shooting

## 常见问题整理

* Executor OOM

```
19:50:23.535 Executor task launch worker for task 3 ERROR org.apache.spark.executor.Executor: Exception in task 0.3 in stage 0.0 (TID 3)
java.lang.OutOfMemoryError: Java heap space
```
默认SparkExecutor
目前给Spark的Driver和Executor都配置了2G内存。如果出现上述OOM错误，可以尝试增加Executor内存
```xml
  <configs>
    <config>
      <name>spark.executor.memory</name>
      <value>4g</value>
    </config>
  </configs>
```

* 读取parquet文件出现很多读取空文件的Task

这个是Spark FileSourceStrategy在将LogicalPlan 转换为 FileSourceScanExec(DataSourceScanExec的子类)时的BUG。
虽然spark在计算每个split时会按照比较理想的参数去计算split，但是物理执行时对应的FileFormat(ParquetFileFormat)读取文件时可能会读到空数据。
```
  def maxSplitBytes(
      sparkSession: SparkSession,
      selectedPartitions: Seq[PartitionDirectory]): Long = {
    val defaultMaxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val defaultParallelism = sparkSession.sparkContext.defaultParallelism
    val totalBytes = selectedPartitions.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
    val bytesPerCore = totalBytes / defaultParallelism

    Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
  }
```

可以通过调大openCostInBytes参数，也可以关闭向FileSourceScanExec算子的转化来处理。
```xml
    <config>
      <name>spark.sql.hive.convertMetastoreParquet</name>
      <value>false</value>
    </config>
```

* 复杂Streaming任务Driver端OOM

Spark UI默认会保留最近1000个executions的执行内容，供用户查看。但是如果每个execution的解析计划比较大，就比较容易造成driver端OOM。
例如，我遇到的一个Streaming任务，每个任务的Spark Plan内存占用约4M，很危险。通过减小`spark.sql.ui.retainedExecutions` 参数后，系统恢复稳定。

## Spark 任务profile

系统内置了async-profile工具用于对Spark程序运行过程进行Profile分析。
因为async-profile工具是基于 perf_events 进行程序采样分析的，所以要求集群机器上开启对应的系统参数。
```
sysctl -w kernel.perf_event_paranoid=1
sysctl -w kernel.kptr_restrict=0
```

在开启executor profile过程中，程序会占用额外的内存资源，有可能会被NodeManager以内存用超而Kill掉。
为了能够将profile结果上传到HDFS，需要修改NodeManager参数`yarn.nodemanager.sleep-delay-before-sigkill.ms=60000`

Spark任务执行过程profile目前提供如下三种方式:

### 直接查看堆栈法

对于简单的任务，可以直接进行spark job管理页面，查看运行慢的executor对应Thread Dump，分析具体那个Thread运行慢导致

### 自动Profile Executor法

对于运行时间较短，但是运行比较慢的任务，可以通过 `--profile` 参数开启对executor进程的Profile。

profile有如下三种profile结果，默认生成JFR文件:
* 通过`--config spark.profile.type=jfr`来指定生成JFR文件
* 通过`--config spark.profile.type=svg`来指定生成SVG火焰图
* 通过`--config spark.profile.type=yourkit`来指定生成Yourkit snapshot文件

executor运行完毕后会将生成的JFR文件上传到HDFS的`/metadata/logs/profile/${applicationId}/${attemptId}/` 路径。

### 手动Profile Executor法

对于运行时间较长，不需要权量进行Profile的Executor可以可以通过 `--profile --config spark.profile.manualprofile=true` 参数手动开启profile。
此时可以进入executor执行的节点的对应进程启动目录，执行 `profile.sh executor`，再依次输入`start` 和`stop` 命令，profile结束后会在当前机器生成火焰图文件 `/tmp/executor_${PID}.svg` 。

PS: 程序内部使用MXBean 进行Profile管理，并提供了Shell工具进行外部管理。如果有JS好的同学可以直接修改Spark Executor页面，增加开始和结束并直接查看火焰图是最方便的了。