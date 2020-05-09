package ca.valencik.sequoia

import org.typelevel.paiges._

object Pretty {

  def prettyQuery[I](q: Query[I, RawName]): Doc = {
    q.cte match {
      case None        => prettyQueryNoWith(q.queryNoWith)
      case Some(withQ) => prettyWith(withQ) + Doc.line + prettyQueryNoWith(q.queryNoWith)
    }
  }

  def prettyWith[I](withQ: With[I, RawName]): Doc = {
    val namedQs = withQ.namedQueries.map(prettyNamedQuery)
    val nqBody  = Doc.intercalate(Doc.char(',') + Doc.line, namedQs)
    Doc.text("WITH") & nqBody
  }

  def prettyNamedQuery[I](nq: NamedQuery[I, RawName]): Doc = {
    val q      = prettyQuery(nq.q)
    val inside = q.tightBracketBy(Doc.char('('), Doc.char(')'))
    Doc.text(nq.n) & Doc.text("AS") & inside
  }

  def prettyQueryNoWith[I](qnw: QueryNoWith[I, RawName]): Doc = {
    val qt = prettyQueryTerm(qnw.queryTerm)
    val limit = qnw.limit match {
      case None      => Doc.empty
      case Some(lim) => Doc.text("LIMIT") & Doc.text(lim.value)
    }
    val order = qnw.orderBy match {
      case None      => Doc.empty
      case Some(ord) => Doc.text("ORDER BY") & prettyOrderBy(ord)
    }
    qt + Doc.lineOrSpace + order + Doc.lineOrSpace + limit
  }

  def prettyOrderBy[I](orderBy: OrderBy[I, RawName]): Doc = {
    val sortItems = orderBy.sortItems.map(prettySortItem)
    Doc.intercalate(Doc.char(',') + Doc.space, sortItems)
  }

  def prettySortItem[I](sortItem: SortItem[I, RawName]): Doc = {
    prettyExpr(sortItem.e)
  }

  def prettyQueryTerm[I](qt: QueryTerm[I, RawName]): Doc =
    qt match {
      case qp: QueryPrimary[I, RawName] => prettyQueryPrimary(qp)
      case so: SetOperation[I, RawName] => prettySetOperation(so)
    }

  def prettyQueryPrimary[I](qp: QueryPrimary[I, RawName]): Doc =
    qp match {
      case qs: QuerySpecification[I, RawName] => prettyQuerySpecification(qs)
      case _                                  => ???
    }

  def prettyQuerySpecification[I](qs: QuerySpecification[I, RawName]): Doc = {
    val clauseEnd = Doc.lineOrEmpty

    val sis        = qs.sis.map(prettySelectItem)
    val selectBody = Doc.intercalate(Doc.char(',') + Doc.line, sis)
    val select     = selectBody.bracketBy(Doc.text("SELECT"), clauseEnd)
    val f          = qs.f.map(prettyRelation)
    val fBody      = Doc.intercalate(Doc.char(',') + Doc.line, f)
    val from       = fBody.bracketBy(Doc.text("FROM"), clauseEnd)
    val where = qs.w match {
      case None       => Doc.empty
      case Some(expr) => Doc.text("WHERE") & prettyExpr(expr)
    }
    select + from + where
  }

  def prettySetOperation[I](so: SetOperation[I, RawName]): Doc = {
    val left  = prettyQueryTerm(so.left)
    val right = prettyQueryTerm(so.right)
    val op    = prettySetOperator(so.op)
    so.setQuantifier match {
      case None             => left + op + right
      case Some(quantifier) => left + op + prettySetQuantifier(quantifier) + right
    }
  }

  def prettySetOperator(so: SetOperator): Doc =
    so match {
      case UNION     => Doc.text("UNION")
      case EXCEPT    => Doc.text("EXCEPT")
      case INTERSECT => Doc.text("INTERSECT")
    }

  def prettySetQuantifier(sq: SetQuantifier): Doc =
    sq match {
      case ALL      => Doc.text("ALL")
      case DISTINCT => Doc.text("DISTINCT")
    }

  def prettyRelation[I](r: Relation[I, RawName]): Doc =
    r match {
      case jr: JoinRelation[I, RawName] => prettyJoinRelation(jr)
      case SampledRelation(_, ar, _)    => prettyAliasedRelation(ar)
    }

  def prettyAliasedRelation[I](ar: AliasedRelation[I, RawName]): Doc = {
    prettyRelationPrimary(ar.rp)
  }

  def prettyJoinRelation[I](jr: JoinRelation[I, RawName]): Doc = {
    val left  = prettyRelation(jr.left)
    val right = prettyRelation(jr.right)
    val joinType = jr.jointype match {
      case CrossJoin => Doc.text("CROSS")
      case FullJoin  => Doc.text("FULL")
      case InnerJoin => Doc.text("INNER")
      case LeftJoin  => Doc.text("LEFT")
      case RightJoin => Doc.text("RIGHT")
    }
    val criteria = jr.criteria.map(prettyJoinCriteria).getOrElse(Doc.empty)
    // TODO does this add a space when criteria is empty?
    left & joinType & Doc.text("JOIN") & right & criteria
  }

  def prettyJoinCriteria[I](jc: JoinCriteria[I, RawName]): Doc =
    jc match {
      case JoinOn(_, be) => Doc.text("ON") & prettyExpr(be)
      case JoinUsing(_, cols) => {
        val cs = cols.map { case uc => Doc.text(uc.value.value) }
        Doc.text("USING") & Doc.intercalate(Doc.char(',') + Doc.space, cs)
      }
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
          case Some(alias) => prettyExpr(expr) + Doc.space + Doc.text("AS") + Doc.text(alias.value)
        }
    }

  def prettyExpr[I](expr: Expression[I, RawName]): Doc =
    expr match {
      case ColumnExpr(_, col)   => Doc.text(col.value.value)
      case IntLiteral(_, value) => Doc.text(value.toString)
      case ArithmeticBinary(_, left, op, right) =>
        prettyExpr(left) & prettyArithmeticOp(op) & prettyExpr(right)
      case LogicalBinary(_, left, op, right) =>
        prettyExpr(left) + Doc.lineOrSpace + prettyBooleanOp(op) & prettyExpr(right)
      case ComparisonExpr(_, left, op, right) =>
        prettyExpr(left) & prettyComparison(op) & prettyExpr(right)
      case DereferenceExpr(_, base, fieldName) =>
        prettyExpr(base) + Doc.char('.') + Doc.text(fieldName)
      case sc: SimpleCase[I, RawName]   => prettySimpleCase(sc)
      case sc: SearchedCase[I, RawName] => prettySearchCase(sc)
      case sl: StringLiteral[I, RawName] =>
        Doc.char('\'') + Doc.text(sl.value) + Doc.char('\'')
      case _ => Doc.text("oops expr")
    }

  def prettySimpleCase[I](sc: SimpleCase[I, RawName]): Doc = {
    val vExp = prettyExpr(sc.exp)
    val wcs  = Doc.intercalate(Doc.space, sc.whenClauses.map(prettyWhenClause))
    val elseC = sc.elseExpression match {
      case None      => Doc.empty
      case Some(exp) => Doc.text("ELSE") & prettyExpr(exp)
    }
    Doc.text("CASE") & vExp & wcs & elseC & Doc.text("END")
  }

  def prettySearchCase[I](sc: SearchedCase[I, RawName]): Doc = {
    val wcs = Doc.intercalate(Doc.space, sc.whenClauses.map(prettyWhenClause))
    val elseC = sc.elseExpression match {
      case None      => Doc.empty
      case Some(exp) => Doc.char('(') + Doc.text("ELSE") & prettyExpr(exp) + Doc.char(')')
    }
    Doc.text("CASE") & wcs & elseC & Doc.text("END")
  }

  def prettyArithmeticOp(op: ArithmeticOperator): Doc =
    op match {
      case ADD      => Doc.char('+')
      case DIVIDE   => Doc.char('/')
      case MODULUS  => Doc.char('%')
      case MULTIPLY => Doc.char('*')
      case SUBTRACT => Doc.char('-')
    }

  def prettyBooleanOp(op: BooleanOperator): Doc =
    op match {
      case AND => Doc.text("AND")
      case OR  => Doc.text("OR")
    }

  def prettyComparison(op: Comparison): Doc =
    op match {
      case EQ  => Doc.char('=')
      case GT  => Doc.char('>')
      case GTE => Doc.text(">=")
      case LT  => Doc.char('<')
      case LTE => Doc.text("<=")
      case NEQ => Doc.text("!=")
    }

  def prettyWhenClause[I](wc: WhenClause[I, RawName]): Doc = {
    val cond = prettyExpr(wc.condition)
    val res  = prettyExpr(wc.result)
    Doc.text("WHEN") & cond & Doc.text("THEN") & res
  }
}
