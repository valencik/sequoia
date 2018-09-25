---
title: Analyzing Presto and Spark SQL with Scala
author: Andrew Valencik
...

# Intro

## Who am I?
<!--
SQL remains the ubiquitous tool of data reporting, analysis, and exploration.
However, sharing context and common usage for datasets across teams is a manual and elective process, leading to poorly repeated patterns and bad habits.
This talk will review a new system, written in Scala, which enables SQL query analysis such as finding commonly joined tables, tracking column lineage, and discovering unused columns.
A primary focus of the effort is to increase data discovery among various data science teams.
-->
  - Data scientist at Shopify
  - Build NLP and search products for our Support Data team
  - Python by day Scala by night (for now...)

## What is the problem?
  - "Big Data"
  - Big Teams
  - Context. Context is the problem.

::: notes
  - Often hear of problems with Big Data in terms of our ability to crunch numbers
  - Gaining and sharing context around data is hard, lots of tables, lots of columns
  - On the order of several thousand tables and many tens of thousands of columns
:::

## Typical ETL Pipeline
  - Data App produces data
  - Some ETL (extracts, transforms, loads)
  - Reporting layer in SQL

::: notes
  - reports are organized, datasets organized, but tribal knowledge is still a thing
  - would be nice to parse SQL for easier analysis
  - e.g. everyone who uses this column applies this filter, you should too
:::

## Presto SQL and Spark SQL
 - Big data distributed SQL engines
 - [Spark SQL][sparksql] forked [Presto SQL][prestosql]
 - Both using ANTLR4 grammar

::: notes
  - Interestingly Spark SQL's grammar is a fork of Presto SQL
  - Whether a good idea or not, this mostly solidfied the approach of using grammars
  - I did however try using the parsers from those projects (see big jar)
  - Worth noting at the time Presto used an older Antlr 4.6 runtime
:::

# ANTLR4

## Why ANTLR4?
  - ANTLR4 is a parser generator toolkit
  - Used by Presto and Spark (and Hive)

::: notes
  - grammars for Presto, Spark, MySQL (which covers just about everything we use at work)
  - in theory this approach leaves me with building the language application and not the parser
:::

## Example
  - Show ANTLR4 [toy example](https://github.com/valencik/antlr4-scala-example)


# Language App

## Query -> Scala
  - Just build a giant tree of case classes for a query. ok now what?
  - `select a, b buz from db.foo`

## Either[ParseFailure, Query]
  ```
  Right(Query(None,
    QueryNoWith(QuerySpecification(
      Select(List(
        SingleColumn(Identifier(A),None),
        SingleColumn(Identifier(B),Some(BUZ)))),
      From(Some(List(Table(QualifiedName(DB.FOO))))),
      Where(None),
      GroupBy(List()),
      Having(None)),
      Some(OrderBy(List())),
      None)))
  ```

## Analysis?
  - Show me all the columns accessed by SQL clause
  - _TODO_ Clause extraction without name resolution

::: notes
 - Hopefully I can write some clause extraction to work on `Query[_, _]`
 - Use this as a lead in to name resolution
:::


# Name Resolution

## Name resolution
  - Resolving Relations
  - Looking up tables in the "catalog"
  - Resolving References
  - `a` in our SELECT clause to `ResolvedReference(db.foo.a)`

::: notes
 - This is a problem I did not know I would have in the beginning
:::

## Query[ResolovableRelation, ResolovableReference]
```
Query(None,
  QueryNoWith(QuerySpecification(
    Select(List(
      SingleColumn(Identifier(ResolvedReference(db.foo.a)),None),
      SingleColumn(Identifier(ResolvedReference(db.foo.b)),Some(BUZ))
    )),
    From(Some(List(Table(ResolvedRelation(db.foo))))),
    ...
```

## Name resolution pt.2
  - Working on simple queries
<!--
  - To work on CTEs we should hopefully be able to just recurse on the sub queries with the same function
  - working on CTEs... well it works on the sub queries but doesn't on the child queries
  - Information is not being shared from the sub query to the child query
-->
  - Spark handles this as part of the analysis on Logical Plans

## Query optimization?
  - `with everything as (select * from foo) select a from everything`
  - _TODO_ Extract snippets on how Spark handles this

::: notes
  - If parts of your query are not needed they shouldn't be run
  - This tool is currently ignorant of these database optimizations
:::

# Future

## Future work at work
  - Column level learning resources
  - Report (SQL) rewriting tools

## Typelevel rewrite
  - Lots of `map` code mixed throughout
  - TempView catalog sounds like State Monad
  - Inspiration from Uber's [queryparser][queryparser]

 [sparksql]: https://github.com/apache/spark/blob/v2.3.2/sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4
 [prestosql]: https://github.com/prestodb/presto/blob/0.211/presto-parser/src/main/antlr4/com/facebook/presto/sql/parser/SqlBase.g4
 [queryparser]: https://github.com/uber/queryparser
