# UDF函数

## UDF函数分类:

* 不包含业务数据处理逻辑的UDF: 建议直接写入本项目内，这样方便UDF的代码复用
* 包含业务处理逻辑的UDF: 这类代码一般不可复用，但是代码仍然要做到统一管理，所以建议和公司内一样，使用独立的repo来管理代码。
接下来说的扩展UDF函数开发流程，指的就是这类UDF函数。

## 扩展UDF函数开发流程

### 编写UDF开发函数 

UDF函数开发规范

* 代码类以`com.wankun.[业务模块].[主业务逻辑]UDF`命名，类名上需要添加`UDFDescription`注解。
* 注解中的`name`属性表示注册的函数名
* 注解中的`returnType`属性表示函数的函数数据类型
* 注解中的`description`属性表示函数的说明
* 原则上所有业务逻辑必须要有明确`description`说明，主函数需要有UT测试类

### 部署UDF函数

编译项目，并上传结果jar到hdfs目录:`/deploy/config/biz-udfs-1.0.jar` (PS: 一般CI可以做到自动化)

```
mvn clean package
hdfs dfs -put -f ./target/biz-udfs-1.0.jar /deploy/config/biz-udfs-1.0.jar
```

# 开发例子说明

下文以开发推荐归因数据的order转换的UDF函数为例，说明开发和使用步骤。

## 开发UDF 函数，并实现UDF4的call方法，实现传入4个参数，输出一个参数的UDF函数

```java
package com.wankun.udfs.recommend;

@UDFDescription(
    name = "attribution_orders",
    returnType = "array<struct< order_id: string, spu_id: string, sku_id: string, quantity: int,\n" +
        "  price: double, payment: double, divide_order_fee: double,  status: string, attribution: string >>",
    description = "在对推荐归因计算时，转换原始trade中的orders数据")
public class AttributionOrdersUDF
    implements UDF4<String, WrappedArray<String>, String, WrappedArray<Row>, WrappedArray<Row>> {
  
@Override
  public WrappedArray<Row> call(String abTarget,
                                WrappedArray<String> priorSpuIds,
                                String dispatchSpuId,
                                WrappedArray<Row> originOrders) throws Exception {
  
                                }
    }
```

## 使用函数

```sql

!set spark.sql.externalUdfClasses = com.wankun.udfs.recommend.AttributionOrdersUDF;

SELECT attribution_orders(ab_target, prior_spu_id, spu_id, orders) as orders 
FROM trade;
```