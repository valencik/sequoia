package ca.valencik.sequoia

import org.typelevel.paiges._

object Pretty {

  def prettyQuery[I](q: Query[I, RawName]): Doc = {
    q.w match {
      case None        => prettyQueryNoWith(q.qnw)
      case Some(withQ) => prettyWith(withQ) + Doc.lineOrSpace + prettyQueryNoWith(q.qnw)
    }
  }

  def prettyWith[I](withQ: With[I, RawName]): Doc = {
    val namedQs = withQ.nqs.map(prettyNamedQuery)
    val nqBody  = Doc.intercalate(Doc.char(',') + Doc.lineOrSpace, namedQs)
    Doc.text("WITH ") + nqBody
  }

  def prettyNamedQuery[I](nq: NamedQuery[I, RawName]): Doc = {
    val q      = prettyQuery(nq.q)
    val inside = q.tightBracketBy(Doc.char('('), Doc.char(')'))
    Doc.text(nq.n) + Doc.text(" AS ") + inside
  }

  def prettyQueryNoWith[I](qnw: QueryNoWith[I, RawName]): Doc = {
    qnw.qt match {
      case qs: QuerySpecification[I, RawName] => prettyQuerySpecification(qs)
      case _                                  => Doc.text("---OOPS QUERYNOWITH---")
    }
  }

  def prettyQuerySpecification[I](qs: QuerySpecification[I, RawName]): Doc = {
    val clauseEnd  = Doc.lineOrSpace
    val sis        = qs.sis.map(prettySelectItem)
    val selectBody = Doc.intercalate(Doc.char(',') + Doc.lineOrSpace, sis)
    val select     = selectBody.bracketBy(Doc.text("SELECT"), clauseEnd)
    val f          = qs.f.map(prettyRelation)
    val fBody      = Doc.intercalate(Doc.char(',') + Doc.space, f)
    val from       = fBody.bracketBy(Doc.text("FROM"), Doc.empty)
    select + from
  }

  def prettyRelation[I](r: Relation[I, RawName]): Doc =
    r match {
      case SampledRelation(_, ar, _) => prettyAliasedRelation(ar)
      case _                         => Doc.text("--OOPS RELATION--")
    }
  def prettyAliasedRelation[I](ar: AliasedRelation[I, RawName]): Doc = {
    prettyRelationPrimary(ar.rp)
  }

  def prettyRelationPrimary[I](rp: RelationPrimary[I, RawName]): Doc =
    rp match {
      case TableName(_, r) => Doc.text(r.value.value)
      case _               => Doc.text("--OOPS RELATION PRIMARY--")
    }

  def prettySelectItem[I](si: SelectItem[I, RawName]): Doc =
    si match {
      case SelectAll(_, _) => Doc.text("*")
      case SelectSingle(_, expr, alias) =>
        alias match {
          case None        => prettyExpr(expr)
          case Some(value) => prettyExpr(expr) + Doc.space + Doc.text("AS") + Doc.text(value.value)
        }
    }

  def prettyExpr[I](expr: Expression[I, RawName]): Doc =
    expr match {
      case ColumnExpr(_, col)   => Doc.text(col.value.value)
      case IntLiteral(_, value) => Doc.text(value.toString)
      case _                    => Doc.text("--OOPS EXPR--")
    }
}

object PrettyApp {
  import Pretty._
  def main(args: Array[String]): Unit = {

    val queryString = "select apple, banana from foo"

    val qD = ParseBuddy
      .parse(queryString)
      .map { pq =>
        if (SimpleRelationToCte.ifQueryHasFoo(pq)) {
          println("Rewriting query...")
          val rewrittenQ = SimpleRelationToCte.setCTE(pq, "myCTE")
          prettyQuery(rewrittenQ)
        } else {
          println("Not rewritting query...")
          prettyQuery(pq)
        }
      }
      .getOrElse(Doc.text("Parse Failure"))

    println(qD.render(80))
    println("---")
    println(qD.render(7))
  }

}
