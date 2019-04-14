Sequoia
=======

Sequoia is a work in progress SQL analysis library.

## Overview

Sequoia is motivated by the desire to better understand data warehouses with many
thousands of tables, columns, and SQL reports (queries). Sequoia will parse SQL,
perform analysis on the resulting tree, and emit statistics on what data
resources that query used. Currently the analysis part has been removed while we
finish work on complete grammar support for parsing Presto queries. Note that
currently we are focusing just on "queries" and not "statements" that might
create or update the underlying data.

Let's look at a quick example. Our entry point is the `parse` function:
```scala
def parse(input: String): Either[ParseFailure, Query[Info, RawName]]
```

We pass `parse` a string that represents our SQL query:
```scala
parse("select x, y from db.foo where y >= 2 order by 1 limit 10")
```

In return we get an `Either` type that is either a `ParseFailure` message or a `Query`.
The above query string gives us a valid `Query` which is pretty printed here:
```scala
Query(
  1,
  None,
  QueryNoWith(
    21,
    QuerySpecification(
      16,
      None,
      NonEmptyList(
        SelectSingle(4, ColumnExpr(2, ColumnRef(3, RawColumnName("x"))), None),
        List(SelectSingle(7, ColumnExpr(5, ColumnRef(6, RawColumnName("y"))), None))
      ),
      Some(
        NonEmptyList(
          SampledRelation(
            11,
            AliasedRelation(10, TableName(9, TableRef(8, RawTableName("db.foo"))), None, None),
            None
          ),
          List()
        )
      ),
      Some(
        ComparisonExpr(
          15,
          ColumnExpr(12, ColumnRef(13, RawColumnName("y"))),
          GTE,
          IntLiteral(14, 2L)
        )
      ),
      None,
      None
    ),
    Some(OrderBy(17, NonEmptyList(SortItem(19, IntLiteral(18, 1L), None, None), List()))),
    Some(Limit(20, "10"))
  )
)
```

## Use Cases
- answering what reports depend on a certain column
- providing statistics on what tables are frequently joined together and how
- determining column lineage

## Stack
Sequoia uses the original [Presto SQL grammar][presto-grammar] and
[ANTLR4][antlr] to generate a parse-tree visitor interface. The generated
visitor interface is then used to build a typed AST out of Scala case classes.
The resulting tree for a SQL query is of type `Query[Info, RawName]`, if the
query passes name resolution validation it is then of type `Query[Info,
ResolvedName]`. Further analysis can be run on these resolved queries.

[antlr]:https://www.antlr.org
[presto-grammar]: https://github.com/prestodb/presto/blob/b88174803cf4a1d3b36918ac91db2465b350ac90/presto-parser/src/main/antlr4/com/facebook/presto/sql/parser/SqlBase.g4
