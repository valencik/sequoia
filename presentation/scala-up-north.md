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

## Arithmetic Lexer
[github.com/valencik/antlr4-scala-example](https://github.com/valencik/antlr4-scala-example)
```
lexer grammar ArithmeticLexer;
WS: [ \t\n]+ -> skip ;
NUMBER: ('0' .. '9') + ('.' ('0' .. '9') +)?;
ADD: '+';
SUB: '-';
MUL: '*';
DIV: '/';
```

## Arithmetic Parser
[github.com/valencik/antlr4-scala-example](https://github.com/valencik/antlr4-scala-example)
```
parser grammar ArithmeticParser;
options { tokenVocab=ArithmeticLexer; }
expr: NUMBER operation NUMBER;
operation: (ADD | SUB | MUL | DIV);
```

## Arithmetic Visitor App
[github.com/valencik/antlr4-scala-example](https://github.com/valencik/antlr4-scala-example)
``` scala
class ArithmeticVisitorApp
  extends ArithmeticParserBaseVisitor[Expr] {

  override def visitExpr(
    ctx: ArithmeticParser.ExprContext): Expression = {

    val operands = ctx.NUMBER().toList.map(_.getText)
    val operand1 = parseDouble(operands(0))
    val operand2 = parseDouble(operands(1))
    val operation = visitOperation(ctx.operation())

    Expression(operand1, operand2, operation)
  }
  ...
```



# Language App

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
  - override all our relevant visitor methods
  - ctx holds info about where in parse tree we are, children, text values
  - we return a type in our Scala AST
:::

## Scala query object
``` scala
Either[ParseFailure, Query]
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
  - Nested Hashmaps
  - schema -> database -> table -> column

## Resolving References
  - `a` in our SELECT clause to `ResolvedReference(db.foo.a)`
``` scala
Query[ResolvableRelation, ResolvableReference]
```
``` scala
Query(None,
  QueryNoWith(QuerySpecification(
    Select(List(
      SingleColumn(Identifier(ResolvedReference(db.foo.a)),None),
      SingleColumn(Identifier(ResolvedReference(db.foo.b)),Some(BUZ))
    )),
    From(Some(List(Table(ResolvedRelation(db.foo))))),
    ...
```

::: notes
 - This is a problem I did not know I would have in the beginning
:::

## Resolved Clauses
  - Working on simple queries
```
SELECT: db.foo.a, db.bar.x
JOIN: db.foo.b, db.bar.y
WHERE: db.foo.c
...
```
<!--
  - To work on CTEs we should hopefully be able to just recurse on the sub queries with the same function
  - working on CTEs... well it works on the sub queries but doesn't on the child queries
  - Information is not being shared from the sub query to the child query
-->

## Query optimization?
  - Query engines rewrite your query
  - Spark handles this as part of the analysis on Logical Plans
``` sql
with everything as (select * from foo) select a from everything
```

::: notes
 - Neither Presto nor Spark will actually select all colums from foo
:::

## Spark's Resolution
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

# EOF

 [sparksql]: https://github.com/apache/spark/blob/v2.3.2/sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4
 [prestosql]: https://github.com/prestodb/presto/blob/0.211/presto-parser/src/main/antlr4/com/facebook/presto/sql/parser/SqlBase.g4
 [queryparser]: https://github.com/uber/queryparser
 [antlr]: http://www.antlr.org
 [catalyst]: https://github.com/apache/spark/blob/v2.3.2/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/Analyzer.scala#L142
