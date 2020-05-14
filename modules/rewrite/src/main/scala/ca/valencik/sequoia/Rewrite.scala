package ca.valencik.sequoia

import pprint.pprintln

object Rewrite {

  def ifRelation[R](pred: Relation[_, R] => Boolean): Query[_, R] => Boolean =
    query => {
      query.queryNoWith.queryTerm match {
        case qs: QuerySpecification[_, R] => qs.from.exists(pred)
        case _                            => false
      }
    }

  def ifTableName[R](pred: R => Boolean): Relation[_, R] => Boolean =
    relation => {
      relation match {
        case sr: SampledRelation[_, R] =>
          sr.ar.rp match {
            case TableName(_, r) => pred(r.value)
            case _               => false
          }
        case _ => false
      }
    }

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
  val ifFoo         = ifTableName[RawName] { r => r.value == "foo" }
  val ifQueryHasFoo = ifRelation(ifFoo)

  def main(args: Array[String]): Unit =
    if (ifQueryHasFoo(inQ))
      pprintln(setCTE(inQ, "myCTE"), height = 10000)
    else
      println("Not query to rewrite")

}
