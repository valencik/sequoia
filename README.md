# Big Jar
If we make a fat jar with `spark-core`, `spark-sql`, and `presto-parser` there are, of course, merge errors but also the resulting JAR is 127 megabytes.

# Thoughts On Use Cases
A proper sql analyzer could give you lineage on Columns, not just Tables.
Existing analysis can only tell us what table a Mode report depends on.
People are often interested in the columns, perhaps some are never queried.
Produce scripts to automatically update all queries or table names in Mode (e.g. nomenclature project).
Stats on what tables are frequently joined together and using what keys.
This could be useful for learning about a data model.

# How does Spark SQL resolve names?
[`ExtractValue`](https://github.com/apache/spark/blob/4dc82259d81102e0cb48f4cb2e8075f80d899ac4/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/complexTypeExtractors.scala#L33)

# Up next
Name resolution has come along way but can still be tripped up on resolving references to colums in named queries.
This is rather expected as we currently do not save the column names in tempView.
With just relation resolution working can we do common join analysis?

# Talk abstract
SQL remains the ubiquitous tool of data reporting, analysis, and exploration.
However, sharing context and common usage for datasets across teams is a manual and elective process, leading to poorly repeated patterns and bad habits.
This talk will review a new system, written in Scala, which enables SQL query analysis such as finding commonly joined tables, tracking column lineage, and discovering unused columns.
A primary focus of the effort is to increase data discovery among various data science teams.

# Alicia Questions
- Why do other people want to know about this?

# Queries of interest

This query works and demonstrates name resolution carrying from one namedQuery to another:
```
with firstq AS
  (SELECT year, month
  FROM hive.raw_kafka.support_router LIMIT 4),
  secondq AS
  (SELECT year, month
  FROM firstq LIMIT 2)
SELECT year, month
FROM secondq
```

This query works and shows of a subqueryExpression:
```
WITH firstq AS
  (SELECT year, month
  FROM hive.raw_kafka.support_router LIMIT 4),
  secondq AS
  (select
    year,
    (select count(month) from firstq where year = 2018 and month = 9) num
  from hive.raw_kafka.support_router
  limit 2)
  select * from secondq
```
