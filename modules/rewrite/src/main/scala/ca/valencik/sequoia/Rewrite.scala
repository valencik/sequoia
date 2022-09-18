package io.pig.sequoia

import pprint.pprintln

object Rewrite {

  def setCTE(q: Query[Int, RawName], name: String): Query[Int, RawName] = {
    val selectItems = q.queryNoWith.queryTerm match {
      case qs: QuerySpecification[Int, RawName] => qs.selectItems
      case _                                    => ???
    }
    val orderBy      = q.queryNoWith.orderBy
    val limit        = q.queryNoWith.limit
    val rewrittenQNW = q.queryNoWith.copy(orderBy = None, limit = None)
    val rewrittenQ   = q.copy(queryNoWith = rewrittenQNW)
    Query(
      14,
      Some(With(13, List(NamedQuery(12, name, None, rewrittenQ)))),
      QueryNoWith(
        23,
        QuerySpecification(
          info = 22,
          setQuantifier = None,
          selectItems = selectItems,
          from = List(
            SampledRelation(
              21,
              AliasedRelation(20, TableName(19, TableRef(18, RawTableName(name))), None, None),
              None
            )
          ),
          where = None,
          groupBy = None,
          having = None
        ),
        orderBy,
        limit
      )
    )
  }
}

object RewriteApp {
  import Rewrite._
  val inQ: Query[Int, RawName] = Query(
    1,
    None,
    QueryNoWith(
      10,
      QuerySpecification(
        9,
        None,
        List(SelectSingle(4, ColumnExpr(2, ColumnRef(3, RawColumnName("a"))), None)),
        List(
          SampledRelation(
            8,
            AliasedRelation(7, TableName(6, TableRef(5, RawTableName("foo"))), None, None),
            None
          )
        ),
        None,
        None,
        None
      ),
      None,
      None
    )
  )

  def main(args: Array[String]): Unit =
    pprintln(setCTE(inQ, "myCTE"), height = 10000)

}
