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