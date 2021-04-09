# 读写外部表

目前系统支持对JDBC数据源的读写，对Kafka的自动数据写入。在写SQL之前需要配置好相关系统参数和外部表参数。系统参数一般只JDBC连接信息，Kafka Broker地址等共用信息，不需要每个任务都进行配置。外部表参数指Mysql中的表名，表的主键等需要具体配置的参数。

## 读写JDBC表

功能说明:
系统支持将JDBC数据源中的一个表或者一个JDBC查询作为一个Spark中表进行读写。设置好JDBC表的相关参数后，通过 `jdbc.[NAMESPACE].[TABLE_NAME]` 名称就可以进行读写了。

使用参数说明:
NAMESPACE: JDBC 连接数据库的标识
TABLE_NAME: Spark SQL中注册的表名

系统参数说明(一般由系统统一配置，多个任务共享该参数):
* [NAMESPACE].url : JDBC连接的地址
* [NAMESPACE].username : JDBC连接的用户名
* [NAMESPACE].password : JDBC连接的密码

读取外部表数据需要配置的参数:
* [NAMESPACE].[TABLE_NAME].numPartitions : 外部表查询的并发数
* [NAMESPACE].[TABLE_NAME].partitionColumn : 外部表并发查询的数据分区字段
* [NAMESPACE].[TABLE_NAME].query : 可选参数，允许将一个JDBC 查询的视图，作为Spark的外部表进行查询

计算结果写入外部表需要配置的参数:
* [NAMESPACE].[TABLE_NAME].uniqueKeys : 外部表的数据更新主键，因为数据写入JDBC表使用的是upsert方式，所以必须提供upsert操作的数据主键

使用示例:

Spark读取 bi 数据库中 stores表数据示例:

```sql
!set bi.stores.numPartitions = 2; 
!set bi.stores.partitionColumn = id;
// query 为可选参数, 如果没有该参数，将直接查询 bi数据库中的stores表，如果有该参数，会查询query的查询结果视图
!set bi.stores.query = """(select * from stores where store_id >10) as subq""";

SELECT  store_id, store_name
FROM    jdbc.bi.stores
WHERE   store_id < 50;
```

Spark 写入 bi 数据库中 stores表数据示例:
```sql
!set bi.stores.uniqueKeys = id;

INSERT INTO jdbc.bi.stores
SELECT 100 as store_id, "store_100" as store_name;
```

## 数据写入Kafka

功能说明:
将SQL的计算结果插入到Kafka中。目前支持将结果自动转换为 avro 和 json 两种数据格式。
设置好Kafka的相关参数后，通过 `kafka.[TABLE_NAME]` 名称就可以向Kafka写数据了。

使用参数说明:
TABLE_NAME: Spark SQL中注册的表名

系统参数说明(一般由系统统一配置，多个任务共享该参数):
* kafka.bootstrap.servers : Kafka集群的Broker地址
* kafka.schema.registry.url : 可选参数，如果Kafka 集群是Confluent版本的Kafka，可以管理Avro格式kafka数据，avro的schema由schema registry进行集中管理。可以配置上对应的schema registry地址。

写入json格式数据需要配置的参数:
* kafka.[TABLE_NAME].kafkaTopic: kafka的topic名称
* kafka.[TABLE_NAME].recordType: 填写 json
* kafka.[TABLE_NAME].maxRatePerPartition : 可选参数，每个spark executor写入kafka的每秒消息数。数据结果数据集比较大，一定要加上速度限制，否则会把kafka写爆掉。

写入avro格式数据需要配置的参数:
* kafka.[TABLE_NAME].kafkaTopic: kafka的topic名称
* kafka.[TABLE_NAME].recordType: 填写 avro
* kafka.[TABLE_NAME].avro.forceCreate : 默认为false， 如果为true，会强制使用计算结果dataframe schema作为kafka avro schema，如果schema registry上已经存在schema则会报错。如果为false，会先从Schema Registry上获取topic的Schema（此时其他avro参数无需配置），如果获取失败，再使用计算结果dataframe schema作为kafka avro schema。
* kafka.[TABLE_NAME].avro.name : 可选参数，如果 `forceCreate` = true, 则必须提供创建avro数据schema需要的 name。
* kafka.[TABLE_NAME].avro.namespace : 可选参数，如果 `forceCreate` = true, 则必须提供创建avro数据schema需要的 namespace。
* kafka.[TABLE_NAME].maxRatePerPartition : 可选参数，每个spark executor写入kafka的每秒消息数。数据结果数据集比较大，一定要加上速度限制，否则会把kafka写爆掉。

使用示例:

向kafka写入json数据示例:

```sql
!set kafka.test_topic.recordType = json;
!set kafka.test_topic.kafkaTopic = test_topic;

INSERT INTO kafka.test_topic
SELECT 100 as id, "user_100" as name;
```

向kafka写入avro数据示例:

```sql
!set kafka.test_topic2.recordType = avro;
!set kafka.test_topic2.kafkaTopic = test_topic2;
-- 根据计算结果DDL自动生成Avro Schema
!set kafka.test_topic2.avro.forceCreate = true;
!set kafka.test_topic2.avro.name = student;
!set kafka.test_topic2.avro.namespace = com.wankun;

INSERT INTO kafka.test_topic2
SELECT 1 as id1, 'wankun' as name1, '男' as sex1, 'PRD' env1;
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