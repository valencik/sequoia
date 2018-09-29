---
title: Analyzing Presto and Spark SQL with Scala
author: Andrew Valencik
...

# Viz Test

## Can you see this?
- [link: switch to dark theme](pres-dark.html#/can-you-see-this)
- [link: switch to light theme](pres-light.html#/can-you-see-this)
- `fixed width`
``` scala
val x = List(1, 2).map(_ + 1)
```
```
raw block
```

# Intro

## Who am I?
<!--
SQL remains the ubiquitous tool of data reporting, analysis, and exploration.
However, sharing context and common usage for datasets across teams is a manual and elective process, leading to poorly repeated patterns and bad habits.
This talk will review a new system, written in Scala, which enables SQL query analysis such as finding commonly joined tables, tracking column lineage, and discovering unused columns.
A primary focus of the effort is to increase data discovery among various data science teams.
-->
- Andrew Valencik (@valencik)
- Data scientist at Shopify
- Build NLP and search products for our Support Data team
- Python by day Scala by night (for now...)

::: notes
Shopify is a complete commerce platform that lets you start, grow, and manage a business
Create and customize an online store
Sell in multiple places, including web, mobile, social media, physical
Manage products, inventory, payments, and shipping
:::

## What is the problem?
- "Big Data"
- Big Teams
- Context is the problem.

::: notes
- Often hear of problems with Big Data in terms of our ability to crunch numbers
- Petabyte data warehouses not run by small teams
- Gaining and sharing context around data is hard, lots of tables, lots of columns
- On the order of several thousand tables and many tens of thousands of columns
:::

## Typical ETL Pipeline
- Data app produces data
- Some ETL (extracts, transforms, loads)
- Reporting layer in SQL

::: notes
- reports are organized, datasets organized, but tribal knowledge is still a thing
- would be nice to parse SQL for easier analysis
- e.g. everyone who uses this column applies this filter, you should too
:::

## Use Cases

- "If I change this column, what reports are affected?"
- Improve data discovery by showing commonly used datasets
- Sub query / CTE search

::: notes
- Our data analysts struggle to find the right data
- How can we improve data discovery?
:::

## Presto SQL and Spark SQL
- Big data distributed SQL engines
- [Spark SQL][sparksql] forked [Presto SQL's][prestosql] grammar
- Both using [ANTLR v4][antlr] grammar

::: notes
- Whether a good idea or not, this mostly solidfied the approach of using grammars
:::



# ANTLR v4

## Why ANTLR v4?
- ANTLR v4 is a parser generator toolkit

::: notes
- grammars for Presto, Spark, MySQL (which covers just about everything we use at work)
- in theory this approach leaves me with building the language application and not the parser
:::

## Flow

0. ANTLR Grammar to Code
1. Query text
2. ANTLR runtime
3. Scala query object

::: notes
- At compile time an sbt plugin runs ANTLR and generates Java code
- At runtime our app gets some query text and calls `parse`
- This turns it over to the ANTLR runtime which builds a parse tree
- We then visit nodes in the parse tree and build our query object
:::

## Lexing Rules
```
...
SELECT: 'SELECT';
...
EQ  : '=';
NEQ : '<>' | '!=';
LT  : '<';
...
IDENTIFIER : (LETTER | '_') (LETTER | DIGIT | '_' | '@' | ':')*;
```

::: notes
- These are the rules for tokenization
- Their syntax is similar to regex, here `SELECT` is literally match
- `IDENTIFIER` is a letter or underscore optionally followed by more letters, digits, and so on.
:::

## Parsing Rules
```
query:  with? queryNoWith;
with: WITH RECURSIVE? namedQuery (',' namedQuery)*;
queryNoWith: queryTerm
      (ORDER BY sortItem (',' sortItem)*)?
      (LIMIT limit=(INTEGER_VALUE | ALL))?;
querySpecification: SELECT setQuantifier? selectItem (',' selectItem)*
      (FROM relation (',' relation)*)?
      (WHERE where=booleanExpression)?
      (GROUP BY groupBy)?
      (HAVING having=booleanExpression)?;
```

::: notes
- Presto SQL grammar a bit over 700 lines
- 40 different rules for "expression"
- one of which is of course, ( query )
:::



# Language App

## Query Text
``` sql
select a, b buz from db.foo
```
``` scala
def parse(input: String):
  Either[ParseFailure, Query[QualifiedName, String]]
```

::: notes
- ANTLR has really nice error reporting so the ParseFailure contains info on what went wrong
:::

## ANTLR Runtime
``` scala
override def visitJoinRelation(
  ctx: SqlBaseParser.JoinRelationContext
  ): QRelation = {
  val left         = visit(ctx.left).asInstanceOf[QRelation]
  val right        = getRight(ctx)
  val joinType     = getJoinType(ctx)
  val joinCriteria = getJoinCriteria(ctx)
  Join(joinType, left, right, joinCriteria)
}
```

::: notes
- ANTLR generates a recursive decent parser in a target language
- Using the Java target we can extend the base visitor class
- there is a method for every parser rule
- here we implement visitJoinRelation which itself visits other nodes linked to in the ctx
- ctx holds info about where in parse tree we are, children, text values
- we return a type in our Scala AST
:::

## Scala query object
``` sql
select a, b buz from db.foo
```
``` scala
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

::: notes
- Here's an example Query structure
- I think there's lots of room for clean up here.
:::

## Scala query object (cont)
``` sql
select a, x from db.foo join db.bar on b = y where c >= 10
```
``` scala
Right(Query(None,
  QueryNoWith(QuerySpecification(
    Select(List(
      SingleColumn(Identifier(A),None),
      SingleColumn(Identifier(X),None))),
    From(Some(List(Join(InnerJoin,
      Table(QualifiedName(DB.FOO)),
      Table(QualifiedName(DB.BAR)),
      Some(JoinOn(
        ComparisonExpression(Identifier(B),EQ,Identifier(Y)))))))),
    Where(Some(
      ComparisonExpression(Identifier(C),GTE,Identifier(10)))),
    ...
```

::: notes
- great, now we have horrific case classes, now what?
:::



# Analysis

## Clauses
- Show me all the columns accessed by SQL clause
``` sql
select a, x from db.foo join db.bar on b = y where c >= 10
```
```
SELECT: A, X
JOIN: B, Y
WHERE: C
...
```

::: notes
- Perhaps the most straight forward seeming aggregation is to group column reference by SQL clause
- The information in the aggregation is not enough. Where does `Y` come from?
:::

## Resolving Relations
- Looking up tables in the "catalog"
- schema -> database -> table -> column
``` scala
def resolveRelations(acc: Catalog,
  q: Query[QualifiedName, String],
  alias: Option[Identifier[String]]):
  Query[ResolvableRelation, String]
```

::: notes
- Resolving the relations that appear in a FROM clause is simple
- resolveRelations takes a catalog and a query and recursively calls itself for all namedqueries
- the catalog gets updated with each named query and finally resolves the child QueryNoWith
:::

## Resolving References
``` sql
select a, x from db.foo join db.bar on b = y where c >= 10
```
- `a` in our SELECT clause to `ResolvedReference(db.foo.a)`

::: notes
- For queries without a With this is straightforward
- All the information we need is in the catalog
:::

## Resolving References (cont)
``` sql
with f as (
  select a from db.foo
)
select a from f
```

::: notes
- Named queries complicate things a bit
- Even in this second query the answer might be straightforward, the outer `a` is the same as `db.foo.a`
- What if the namedquery has joins or more complicated select expressions
:::

## Aside: What does Spark do?
- Spark handles this as part of the analysis on Logical Plans
``` sql
with everything as (
  select * from foo
)
select a from everything
```
```
CTE [everything]
:  +- 'SubqueryAlias everything
:     +- 'Project [*]
:        +- 'UnresolvedRelation `foo`
+- 'Project ['a]
   +- 'UnresolvedRelation `everything`
```

::: notes
- Here we see the logical plan with no optimizations
- That CTE gets substituted into the query and ultimately optimized away
- Neither Presto nor Spark will actually select all colums from foo
:::

## Resolved Clauses
``` sql
with everything as (
  select * from foo
)
select a from everything
```
```
SELECT: db.foo.a, db.foo.b, db.foo.c,
JOIN:
WHERE:
...
```

::: notes
- So this analysis tool is db optimization unaware and has this limitation
- This particular type of query is uncommon
- But I am not sure what other issues from this limitation there may be
:::

## Resolved Clauses (cont)
``` sql
select a, x from db.foo join db.bar on b = y where c >= 10
```
```
SELECT: db.foo.a, db.bar.x
JOIN: db.foo.b, db.bar.y
WHERE: db.foo.c
...
```


# Future

## Future work at work
- Column level advice (common filter)
- Report (SQL) rewriting tools
- A more fp rewrite

## Sequoia

https://github.com/valencik/sequoia


# EOF

# Appendix

## Aside: Query Optimization
```
== Analyzed Logical Plan ==
a: string
Project [a#7]
+- SubqueryAlias everything
   +- Project [a#7, b#8, c#9]
      +- SubqueryAlias foo
         +- LocalRelation [a#7, b#8, c#9]
```
```
== Optimized Logical Plan ==
LocalRelation [a#7]
```

::: notes
- That CTE gets substituted into the query and ultimately optimized away
- Neither Presto nor Spark will actually select all colums from foo
:::

## Aside: Spark's Resolution
[`...catalyst/analysis/Analyzer.scala`][catalyst]
``` scala
...
Batch("Substitution", fixedPoint,
  CTESubstitution,
  WindowsSubstitution,
  EliminateUnions,
  new SubstituteUnresolvedOrdinals(conf)),
Batch("Resolution", fixedPoint,
  ResolveTableValuedFunctions ::
  ResolveRelations ::
  ResolveReferences ::
...
```

::: notes
- The catalyst analyzer does a decent amount of manipulation to the Logical plan before resolution
:::
 [sparksql]: https://github.com/apache/spark/blob/v2.3.2/sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4
 [prestosql]: https://github.com/prestodb/presto/blob/0.211/presto-parser/src/main/antlr4/com/facebook/presto/sql/parser/SqlBase.g4
 [queryparser]: https://github.com/uber/queryparser
 [antlr]: http://www.antlr.org
 [catalyst]: https://github.com/apache/spark/blob/v2.3.2/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/Analyzer.scala#L142
