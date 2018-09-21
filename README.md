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

# Talk abstract
SQL remains the ubiquitous tool of data reporting, analysis, and exploration.
However, sharing context and common usage for datasets across teams is a manual and elective process, leading to poorly repeated patterns and bad habits.
This talk will review a new system, written in Scala, which enables SQL query analysis such as finding commonly joined tables, tracking column lineage, and discovering unused columns.
A primary focus of the effort is to increase data discovery among various data science teams.

# Alicia Questions
- Why do other people want to know about this?

# Presentation notes
- who am i
- what is the problem?
  - context is hard, lots of tables, lots of columns
  - reports are organized, datasets organized, but tribal knowledge is still a thing
  - would be nice to parse SQL for easier analysis
  - everyone who uses this column applies this filter, you should too
- Presto SQL and Spark SQL
  - Interestingly Spark SQL's grammar is a fork of Presto SQL
  - Whether a good idea or not, this mostly solidfied the approach of using grammars
  - I did however try using the parsers from those projects (see big jar)
- ANTLR4
  - Show toy example and giter8 template
  - grammars for Presto, Spark, MySQL (which covers just about everything we use at work)
  - in theory this approach leaves me with building the language application and not the parser
- Language App
  - Just build a giant tree of case classes for a query. ok now what?
  - Show me all the columns accessed by SQL clause
- Name resolution
  - Working on simple queries
  - To work on CTEs we should hopefully be able to just recurse on the sub queries with the same function
  - working on CTEs... well it works on the sub queries but doesn't on the child queries
  - Information is not being shared from the sub query to the child query
  - Spark handles this as part of the analysis on Logical Plans
- Query optimization
  - If parts of your query are not needed they shouldn't be run
  - This tool is currently ignorant of these database optimizations
  - query: with everything as (select * from foo) select a from everything
- What can it do?
  - MAYBE aggregate columns used by clause
  - MAYBE aggregate some stats on joins specifically
  - BONUS sql query your sql queries

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
