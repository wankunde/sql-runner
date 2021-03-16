# SQL命令中的Hint扩展

系统通过扩展支持Hint，实现了外部数据源的读和写，下面对主要的扩展Hint做说明

## JDBC_VIEW([DB_TAG], [RELATION_NAME])

功能说明:
通过JDBC连接一个外部数据表，或者一个JDBC查询作为一个Spark中的视图。

Hint参数说明:

* DB_TAG: JDBC连接Tag
* RELATION_NAME: 如果已经配置`DB_TAG.RELATION_NAME.query` 参数，则会根据这个DB查询的Query作为一个Spark中的视图。如果没有配置，则把`DB_TAG`数据中的`RELATION_NAME`表作为一个Spark中的视图。

辅助参数说明:
* [DB_TAG].[RELATION_NAME].numPartitions : 控制数据库查询的并行度
* [DB_TAG].[RELATION_NAME].partitionColumn : 可选参数, 如果使用多线程进行数据查询，每个分区会根据查询该列数值的hash值取模算法，进行散列查询。


使用示例:

以下SQL会连接和读取`bi`数据库中的`stores`表数据作为Spark的计算数据源进行数据处理。

```sql
// query 为可选参数, DB_TAG = bi, RELATION_NAME = stores
!set bi.stores.query = """(select * from stu where id<3) as subq"""; 
!set bi.stores.partitionColumn = id;
!set bi.stores.numPartitions = 2;

INSERT OVERWRITE TABLE tmp.ods_bi partition(dt='${date|yyyyMMdd}')
SELECT  /*+ JDBC_VIEW(bi, stores) */ id, store_id
FROM    stores
WHERE   id < 50;
```

说明:

* Spark连接JDBC_VIEW默认为单线程，在访问数据量较大的表时，容易造成访问超时。此时，可以通过设置`partitionColumn`参数和`numPartitions`参数来支持Spark并发访问数据库; 数据分区算法使用除余算法，而不是spark内置算法，所以不需要指定 `lowerBound`和`upperBound`参数。

## JDBC_SINK([DB_TAG], [TABLE_NAME], [RELATION_NAME])

功能说明:
通过JDBC连接一个外部数据表，实现将SQL的程序结果以upsert方式插入到一个外部表中。
程序会根据目标的schema 和计算结果的数据schema动态生成 upsert语句。

Hint参数说明:

* DB_TAG: JDBC数据库的连接标识
* TABLE_NAME: 如果没有配置`RELATION_NAME`，`RELATION_NAME`=`TABLE_NAME`，把`TAG`数据库中的`TABLE_NAME`表创建为一个名为`RELATION_NAME` 的Spark视图。
* RELATION_NAME: JDBC表别名，即允许对JDBC输出表取别名。非必须参数，不填写时，和 TABLE_NAME 参数保持一致。

辅助参数说明:
* [DB_TAG].[RELATION_NAME].unique.keys : 必填参数，数据插入和更新的表的主键字段。有多个字段时，使用逗号分隔。
* [DB_TAG].[RELATION_NAME].numPartitions : 控制数据库更新操作的并行度

使用示例:

把Spark计算结果插入到 `mysql`库中存在`stu` 数据表 中

```sql
-- 假设 DB_TAG = mysql, RELATION_NAME = stu
!set mysql.stu.unique.keys = id;

WITH t as (
    SELECT 100 as id, "user_100" as name
    UNION ALL
    SELECT 101 as id, "user_101" as name
)
INSERT INTO stu
SELECT /*+ JDBC_SINK(mysql, stu) */ *
FROM t;
```

```sql
!set mysql.stu.unique.keys = id;

WITH t as (
    SELECT 100 as id, "user_100" as name
    UNION ALL
    SELECT 101 as id, "user_101" as name
)
INSERT INTO stu2
SELECT /*+ JDBC_SINK(mysql, stu, stu2) */ *
FROM t;
```

## KAFKA_SINK(RELATION_NAME)

功能说明:
将SQL的程序结果插入到Kafka中。目前支持将结果自动转换为 avro 和 json 两种数据格式。
目前支持的kafka 全局配置参数有 `kafka.bootstrap.servers`, `kafka.schema.registry.url`。

Hint参数说明:

* RELATION_NAME: Spark中用于数据插入的视图名。

辅助参数说明:
* kafka.recordType : 发送到kafka中的数据格式，支持 `json` 和 `avro` 两种格式。
* kafka.[RELATION_NAME].kafkaTopic : kafka 的topic 名称。
* kafka.[RELATION_NAME].avro.name : 如果数据为avro格式，可以指定avro 的名字。
* kafka.[RELATION_NAME].avro.namespace : 如果数据为avro格式，可以指定avro 的namespace。
* kafka.[RELATION_NAME].avro.forceCreate : 默认为false， 如果为true，会强制使用计算结果dataframe schema作为kafka avro schema，如果schema registry上已经存在schema则会报错。如果为false，会先从Schema Registry上获取topic的Schema（此时其他avro参数无需配置），如果获取失败，再使用计算结果dataframe schema作为kafka avro schema。
* kafka.[RELATION_NAME].maxRatePerPartition : 每个spark executor写入kafka的每秒消息数。数据结果数据集比较大，一定要加上速度限制，否则会把kafka写爆掉。


使用示例:

```sql
-- 假设 RELATION_NAME = stu
!set kafka.recordType = json;
!set kafka.stu.kafkaTopic = test_wankun;

WITH t as (
    SELECT 100 as id, "user_100" as name
    UNION ALL
    SELECT 101 as id, "user_101" as name
)
INSERT INTO stu
SELECT /*+ KAFKA_SINK(stu) */ *
FROM t;

!set tag = kafka;
!set kafka.recordType = avro;
!set kafka.stu.kafkaTopic = test_wankun2;
-- 根据计算结果DDL自动生成Avro Schema
!set kafka.stu.avro.forceCreate = true;
!set kafka.stu.avro.name = student;
!set kafka.stu.avro.namespace = com.wankun;

INSERT INTO stu
SELECT /*+ KAFKA_SINK(stu) */
      1 as id1, 'wankun' as name1,
       '男' as sex1, 'PRD' env1;
```

注意：
Avro 1.8.* 版本对于Enum类型支持有问题。比较trick的解决办法是直接将Spark安装环境下的avro包删除掉.
因为hive-exec-1.1.0-cdh5.13.3.jar包 assemb 了 avro 的 1.7.6-cdh5.13.3的包，所以运行完全没问题。 

## DINGDING_SINK(DING_BOT)

功能说明:
将SQL的程序结果通过钉钉机器人发送到钉钉群。

参数说明:

* DING_BOT: 钉钉机器人名称

辅助参数说明:
!data_alert.title=chatlog白名单店铺没有拉取到chatlog数据;
!data_alert.pattern={store_id}: {store_name};

* ${DING_BOT} : 钉钉机器人Token
* ${DING_BOT}.title : 钉钉群信息Title
* ${DING_BOT}.pattern : 钉钉信息格式

使用示例:

```sql
!data_alert.title=trade信息告警;
!data_alert.pattern={store_id}: {store_name};

select /*+ DINGDING_SINK(data_alert) */
        distinct a.store_id, a.store_name
FROM trade a
WHERE a.dt='${date|yyyyMMdd}';
```

## EMAIL_SINK(EMAIL_BOT)

功能说明:
将SQL的程序结果发送邮件。

参数说明:

* EMAIL_BOT: Email发送机器人名称

辅助参数说明:

* ${EMAIL_BOT} : Email 机器人标识
* ${EMAIL_BOT}.columns : 需要取结果数据中的哪些字段
* ${EMAIL_BOT}.columnNames : 结果数据中字段对应的中文名
* ${EMAIL_BOT}.subject : 邮件标题
* ${EMAIL_BOT}.email-to : 邮件接收人地址，多个地址使用逗号分割
* ${EMAIL_BOT}.email-cc : 邮件抄送人地址，多个地址使用逗号分割

使用示例:

```sql
!set email.columns={store_id}, {store_name};
!set email.columnNames=ID,名称;
!set email.subject = 测试邮件;
!set email.email-to = test-to@abc.com;
!set email.email-cc = test-cc@abc.com;

select /*+ EMAIL_SINK(email) */
        distinct a.store_id, a.store_name
FROM trade a
WHERE a.dt='${date|yyyyMMdd}';
```