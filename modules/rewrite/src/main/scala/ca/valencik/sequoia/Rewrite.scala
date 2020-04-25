package ca.valencik.sequoia

import pprint.pprintln

object SimpleRelationToCte {
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

  def ifRelation[R](pred: Relation[_, R] => Boolean): Query[_, R] => Boolean =
    query => {
      query.qnw.qt match {
        case qs: QuerySpecification[_, R] => qs.f.exists(pred)
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

  def setCTE(q: Query[Int, RawName], name: String): Query[Int, RawName] =
    Query(
      14,
      Some(With(13, List(NamedQuery(12, name, None, q)))),
      QueryNoWith(
        23,
        QuerySpecification(
          22,
          None,
          List(SelectSingle(17, ColumnExpr(15, ColumnRef(16, RawColumnName("a"))), None)),
          List(
            SampledRelation(
              21,
              AliasedRelation(20, TableName(19, TableRef(18, RawTableName(name))), None, None),
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
