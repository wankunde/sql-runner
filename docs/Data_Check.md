# 数据质量检查

## DATA_CHECK([ALERT_MESSAGE], [CHECK_EXPRESSION])

功能说明:
系统扩展了一个名为 DATA_CHECK的 SQL Hint，用户可以自定义数据检查表达式，如果该Boolean表达式计算结果返回false，会进行钉钉告警。

参数说明:

* ALERT_MESSAGE: 数据检查失败时，告警信息
* CHECK_EXPRESSION: 数据检查Boolean表达式。表达式中可以使用当前SQL中可以访问的任意列数据。

使用示例:

以下SQL会对比trade表中最近两天的店铺数量，如果差值大于100，则进行钉钉告警。

```sql
!set diff_num = 100;

WITH raw AS (
    SELECT
        count(DISTINCT store_id) AS stores
    FROM trade
    WHERE dt in ('${date-1d|yyyyMMdd}', '${date|yyyyMMdd}')
    GROUP BY dt)
SELECT /*+DATA_CHECK('交易表今日与昨日店铺数量差值大于${diff_num}', 'diff < ${diff_num}') */
        max(stores) - min(stores) AS diff
FROM raw;
```
