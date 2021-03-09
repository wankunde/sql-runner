# Hive表数据写入时排序索引

如果程序是将计算结果插入到hive表，运行前会先进行判断hive表是否有 `index_column` 属性，如果有，结果数据会根据这个列进行数据排序。
后续对该表的数据查询，如果带有`index_column`列的查询条件，数据会进行非常多的文件级过滤。

hive表设置索引列方法:

`alter table t set tblproperties('index_column'='col');`