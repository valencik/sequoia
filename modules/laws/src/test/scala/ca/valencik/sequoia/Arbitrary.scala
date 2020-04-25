package ca.valencik.sequoia

import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Arbitrary.{arbitrary => getArbitrary}

object arbitrary {
  implicit def arbTableRef[I: Arbitrary, R: Arbitrary]: Arbitrary[TableRef[I, R]] =
    Arbitrary(for { i <- getArbitrary[I]; r <- getArbitrary[R] } yield TableRef(i, r))

  implicit def arbColumnRef[I: Arbitrary, R: Arbitrary]: Arbitrary[ColumnRef[I, R]] =
    Arbitrary(for { i <- getArbitrary[I]; r <- getArbitrary[R] } yield ColumnRef(i, r))

  implicit def arbUsingColumn[I: Arbitrary, R: Arbitrary]: Arbitrary[UsingColumn[I, R]] =
    Arbitrary(for { i <- getArbitrary[I]; r <- getArbitrary[R] } yield UsingColumn(i, r))

  implicit def arbColumnAlias[I: Arbitrary]: Arbitrary[ColumnAlias[I]] =
    Arbitrary(for { i <- getArbitrary[I]; s <- getArbitrary[String] } yield ColumnAlias(i, s))

  implicit def arbTableAlias[I: Arbitrary]: Arbitrary[TableAlias[I]] =
    Arbitrary(for { i <- getArbitrary[I]; s <- getArbitrary[String] } yield TableAlias(i, s))

  implicit def arbQuery[I: Arbitrary, R: Arbitrary]: Arbitrary[Query[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      w <- Gen.frequency((19, Gen.const(None)), (1, Gen.some(getArbitrary[With[I, R]])))
      q <- getArbitrary[QueryNoWith[I, R]]
    } yield Query(i, w, q))

  implicit def arbWith[I: Arbitrary, R: Arbitrary]: Arbitrary[With[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      n <- Gen.resize(3, getArbitrary[List[NamedQuery[I, R]]])
    } yield With(i, n))

  implicit def arbQueryNoWith[I: Arbitrary, R: Arbitrary]: Arbitrary[QueryNoWith[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      q <- getArbitrary[QueryTerm[I, R]]
      o <- Gen.frequency((8, Gen.const(None)), (2, Gen.some(getArbitrary[OrderBy[I, R]])))
      l <- Gen.option(getArbitrary[Limit[I]])
    } yield QueryNoWith(i, q, o, l))

  implicit def arbOrderBy[I: Arbitrary, R: Arbitrary]: Arbitrary[OrderBy[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      s <- Gen.resize(3, getArbitrary[List[SortItem[I, R]]])
    } yield OrderBy(i, s))

  implicit def arbOrdering: Arbitrary[Ordering] =
    Arbitrary(Gen.oneOf(Gen.const(ASC), Gen.const(DESC)))

  implicit def arbNullOrdering: Arbitrary[NullOrdering] =
    Arbitrary(Gen.oneOf(Gen.const(FIRST), Gen.const(LAST)))

  implicit def arbSortItem[I: Arbitrary, R: Arbitrary]: Arbitrary[SortItem[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      e <- getArbitrary[Expression[I, R]]
      o <- Gen.option(getArbitrary[Ordering])
      n <- Gen.option(getArbitrary[NullOrdering])
    } yield SortItem(i, e, o, n))

  implicit def arbLimit[I: Arbitrary]: Arbitrary[Limit[I]] =
    Arbitrary(for { i <- getArbitrary[I]; v <- getArbitrary[String] } yield Limit(i, v))

  implicit def arbQueryTerm[I: Arbitrary, R: Arbitrary]: Arbitrary[QueryTerm[I, R]] =
    Arbitrary(
      Gen.frequency((4, getArbitrary[QueryPrimary[I, R]]), (1, getArbitrary[SetOperation[I, R]]))
    )

  implicit def arbSetOperation[I: Arbitrary, R: Arbitrary]: Arbitrary[SetOperation[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      l <- getArbitrary[QueryTerm[I, R]]
      o <- getArbitrary[SetOperator]
      s <- Gen.option(getArbitrary[SetQuantifier])
      r <- getArbitrary[QueryTerm[I, R]]
    } yield SetOperation(i, l, o, s, r))

  implicit def arbSetOperator: Arbitrary[SetOperator] =
    Arbitrary(Gen.oneOf(Gen.const(INTERSECT), Gen.const(UNION), Gen.const(EXCEPT)))

  implicit def arbSetQuantifier: Arbitrary[SetQuantifier] =
    Arbitrary(Gen.oneOf(Gen.const(DISTINCT), Gen.const(ALL)))

  implicit def arbQueryPrimary[I: Arbitrary, R: Arbitrary]: Arbitrary[QueryPrimary[I, R]] =
    Arbitrary(
      Gen.frequency(
        (8, getArbitrary[QuerySpecification[I, R]]),
        (2, getArbitrary[QueryPrimaryTable[I, R]]),
        (1, getArbitrary[InlineTable[I, R]]),
        (1, getArbitrary[SubQuery[I, R]])
      )
    )

  implicit def arbQueryPrimaryTable[I: Arbitrary, R: Arbitrary]
      : Arbitrary[QueryPrimaryTable[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      t <- getArbitrary[TableRef[I, R]]
    } yield QueryPrimaryTable(i, t))

  implicit def arbInlineTable[I: Arbitrary, R: Arbitrary]: Arbitrary[InlineTable[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      v <- Gen.resize(3, getArbitrary[List[Expression[I, R]]])
    } yield InlineTable(i, v))

  implicit def arbSubQuery[I: Arbitrary, R: Arbitrary]: Arbitrary[SubQuery[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      q <- getArbitrary[QueryNoWith[I, R]]
    } yield SubQuery(i, q))

  implicit def arbQuerySpecification[I: Arbitrary, R: Arbitrary]
      : Arbitrary[QuerySpecification[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      q <- Gen.frequency((8, Gen.const(None)), (2, Gen.some(getArbitrary[SetQuantifier])))
      s <- Gen.resize(5, getArbitrary[List[SelectItem[I, R]]])
      f <- Gen.resize(2, getArbitrary[List[Relation[I, R]]])
      w <- Gen.frequency((8, Gen.const(None)), (2, Gen.some(getArbitrary[LogicalBinary[I, R]])))
      g <- Gen.frequency((8, Gen.const(None)), (2, Gen.some(getArbitrary[GroupBy[I, R]])))
      h <- Gen.frequency((8, Gen.const(None)), (2, Gen.some(getArbitrary[LogicalBinary[I, R]])))
    } yield QuerySpecification(i, q, s, f, w, g, h))

  implicit def arbGroupBy[I: Arbitrary, R: Arbitrary]: Arbitrary[GroupBy[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      s <- Gen.frequency((8, Gen.const(None)), (2, Gen.some(getArbitrary[SetQuantifier])))
      g <- Gen.resize(5, getArbitrary[List[GroupingElement[I, R]]])
    } yield GroupBy(i, s, g))

  implicit def arbGroupingElement[I: Arbitrary, R: Arbitrary]: Arbitrary[GroupingElement[I, R]] =
    Arbitrary(
      Gen.frequency(
        (5, getArbitrary[SingleGroupingSet[I, R]]),
        (2, getArbitrary[Rollup[I, R]]),
        (2, getArbitrary[Cube[I, R]]),
        (1, getArbitrary[MultipleGroupingSets[I, R]])
      )
    )

  implicit def arbSingleGroupingSet[I: Arbitrary, R: Arbitrary]
      : Arbitrary[SingleGroupingSet[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      g <- getArbitrary[GroupingSet[I, R]]
    } yield SingleGroupingSet(i, g))

  implicit def arbRollup[I: Arbitrary, R: Arbitrary]: Arbitrary[Rollup[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      e <- Gen.resize(2, getArbitrary[List[Expression[I, R]]])
    } yield Rollup(i, e))

  implicit def arbCube[I: Arbitrary, R: Arbitrary]: Arbitrary[Cube[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      e <- Gen.resize(2, getArbitrary[List[Expression[I, R]]])
    } yield Cube(i, e))

  implicit def arbMultipleGroupingSets[I: Arbitrary, R: Arbitrary]
      : Arbitrary[MultipleGroupingSets[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      g <- Gen.resize(2, getArbitrary[List[GroupingSet[I, R]]])
    } yield MultipleGroupingSets(i, g))

  implicit def arbGroupingSet[I: Arbitrary, R: Arbitrary]: Arbitrary[GroupingSet[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      g <- Gen.resize(2, getArbitrary[List[Expression[I, R]]])
    } yield GroupingSet(i, g))

  implicit def arbNamedQuery[I: Arbitrary, R: Arbitrary]: Arbitrary[NamedQuery[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      n <- getArbitrary[String]
      c <- Gen.frequency((8, Gen.const(None)), (2, Gen.some(getArbitrary[ColumnAliases[I]])))
      q <- getArbitrary[Query[I, R]]
    } yield NamedQuery(i, n, c, q))

  implicit def arbSelectItem[I: Arbitrary, R: Arbitrary]: Arbitrary[SelectItem[I, R]] =
    Arbitrary(
      Gen.frequency((1, getArbitrary[SelectAll[I, R]]), (2, getArbitrary[SelectSingle[I, R]]))
    )

  implicit def arbSelectAll[I: Arbitrary, R: Arbitrary]: Arbitrary[SelectAll[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      r <- Gen.frequency((9, Gen.const(None)), (1, Gen.some(getArbitrary[TableRef[I, R]])))
    } yield SelectAll(i, r))

  implicit def arbSelectSingle[I: Arbitrary, R: Arbitrary]: Arbitrary[SelectSingle[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      e <- getArbitrary[Expression[I, R]]
      c <- Gen.frequency((8, Gen.const(None)), (2, Gen.some(getArbitrary[ColumnAlias[I]])))
    } yield SelectSingle(i, e, c))

  implicit def arbRelation[I: Arbitrary, R: Arbitrary]: Arbitrary[Relation[I, R]] =
    Arbitrary(
      Gen.frequency((1, getArbitrary[JoinRelation[I, R]]), (9, getArbitrary[SampledRelation[I, R]]))
    )

  implicit def arbJoinRelation[I: Arbitrary, R: Arbitrary]: Arbitrary[JoinRelation[I, R]] =
    Arbitrary(for {
      i  <- getArbitrary[I]
      jt <- getArbitrary[JoinType]
      l  <- getArbitrary[Relation[I, R]]
      r  <- getArbitrary[Relation[I, R]]
      c  <- Gen.frequency((9, Gen.const(None)), (1, Gen.some(getArbitrary[JoinCriteria[I, R]])))
    } yield JoinRelation(i, jt, l, r, c))

  implicit def arbJoinType: Arbitrary[JoinType] =
    Arbitrary(
      Gen.oneOf(
        Gen.const(InnerJoin),
        Gen.const(LeftJoin),
        Gen.const(RightJoin),
        Gen.const(FullJoin),
        Gen.const(CrossJoin)
      )
    )

  implicit def arbJoinCriteria[I: Arbitrary, R: Arbitrary]: Arbitrary[JoinCriteria[I, R]] =
    Arbitrary(Gen.frequency((8, getArbitrary[JoinOn[I, R]]), (2, getArbitrary[JoinUsing[I, R]])))

  implicit def arbJoinOn[I: Arbitrary, R: Arbitrary]: Arbitrary[JoinOn[I, R]] =
    Arbitrary(for { i <- getArbitrary[I]; e <- getArbitrary[Expression[I, R]] } yield JoinOn(i, e))

  implicit def arbJoinUsing[I: Arbitrary, R: Arbitrary]: Arbitrary[JoinUsing[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      c <- Gen.resize(5, getArbitrary[List[UsingColumn[I, R]]])
    } yield JoinUsing(i, c))

  implicit def arbSampledRelation[I: Arbitrary, R: Arbitrary]: Arbitrary[SampledRelation[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      a <- getArbitrary[AliasedRelation[I, R]]
      t <- Gen.frequency((9, Gen.const(None)), (1, Gen.some(getArbitrary[TableSample[I, R]])))
    } yield SampledRelation(i, a, t))

  implicit def arbTableSample[I: Arbitrary, R: Arbitrary]: Arbitrary[TableSample[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      t <- getArbitrary[SampleType]
      e <- getArbitrary[Expression[I, R]]
    } yield TableSample(i, t, e))

  implicit def arbSampleType: Arbitrary[SampleType] =
    Arbitrary(Gen.oneOf(Gen.const(BERNOULLI), Gen.const(SYSTEM)))

  implicit def arbAliasedRelation[I: Arbitrary, R: Arbitrary]: Arbitrary[AliasedRelation[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      r <- getArbitrary[RelationPrimary[I, R]]
      a <- Gen.frequency((9, Gen.const(None)), (1, Gen.some(getArbitrary[TableAlias[I]])))
      c <- Gen.frequency((9, Gen.const(None)), (1, Gen.some(getArbitrary[ColumnAliases[I]])))
    } yield AliasedRelation(i, r, a, c))

  implicit def arbColumnAliases[I: Arbitrary]: Arbitrary[ColumnAliases[I]] =
    Arbitrary(for {
      c <- Gen.resize(5, getArbitrary[List[ColumnAlias[I]]])
    } yield ColumnAliases(c))

  implicit def arbRelationPrimary[I: Arbitrary, R: Arbitrary]: Arbitrary[RelationPrimary[I, R]] =
    Arbitrary(
      Gen.frequency(
        (5, getArbitrary[TableName[I, R]]),
        (1, getArbitrary[Unnest[I, R]]),
        (1, getArbitrary[SubQueryRelation[I, R]])
      )
    )

  implicit def arbTableName[I: Arbitrary, R: Arbitrary]: Arbitrary[TableName[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      r <- getArbitrary[TableRef[I, R]]
    } yield TableName(i, r))

  implicit def arbUnnest[I: Arbitrary, R: Arbitrary]: Arbitrary[Unnest[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      e <- Gen.resize(1, getArbitrary[List[Expression[I, R]]])
      o <- getArbitrary[Boolean]
    } yield Unnest(i, e, o))

  implicit def arbSubQueryRelation[I: Arbitrary, R: Arbitrary]: Arbitrary[SubQueryRelation[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      q <- getArbitrary[Query[I, R]]
    } yield SubQueryRelation(i, q))

  implicit def arbLateralRelation[I: Arbitrary, R: Arbitrary]: Arbitrary[LateralRelation[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      q <- getArbitrary[Query[I, R]]
    } yield LateralRelation(i, q))

  implicit def arbParenthesizedRelation[I: Arbitrary, R: Arbitrary]
      : Arbitrary[ParenthesizedRelation[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      r <- getArbitrary[Relation[I, R]]
    } yield ParenthesizedRelation(i, r))

  implicit def arbExpression[I: Arbitrary, R: Arbitrary]: Arbitrary[Expression[I, R]] =
    Arbitrary(
      Gen.frequency(
        (1, getArbitrary[LogicalBinary[I, R]]),
        (25, getArbitrary[Predicate[I, R]]),
        (25, getArbitrary[ValueExpression[I, R]])
      )
    )

  implicit def arbLogicalBinary[I: Arbitrary, R: Arbitrary]: Arbitrary[LogicalBinary[I, R]] =
    Arbitrary(for {
      i  <- getArbitrary[I]
      l  <- getArbitrary[Expression[I, R]]
      op <- getArbitrary[BooleanOperator]
      r  <- getArbitrary[Expression[I, R]]
    } yield LogicalBinary(i, l, op, r))

  implicit def arbPredicate[I: Arbitrary, R: Arbitrary]: Arbitrary[Predicate[I, R]] =
    Arbitrary(
      Gen.frequency(
        (5, getArbitrary[NotPredicate[I, R]]),
        (5, getArbitrary[ComparisonExpr[I, R]]),
        (1, getArbitrary[QuantifiedComparison[I, R]]),
        (1, getArbitrary[Between[I, R]]),
        (1, getArbitrary[InList[I, R]]),
        (1, getArbitrary[InSubQuery[I, R]]),
        (3, getArbitrary[Like[I, R]]),
        (25, getArbitrary[NullPredicate[I, R]]),
        (5, getArbitrary[DistinctFrom[I, R]])
      )
    )

  implicit def arbNotPredicate[I: Arbitrary, R: Arbitrary]: Arbitrary[NotPredicate[I, R]] =
    Arbitrary(
      for { i <- getArbitrary[I]; v <- getArbitrary[Expression[I, R]] } yield NotPredicate(i, v)
    )

  implicit def arbComparisonExpr[I: Arbitrary, R: Arbitrary]: Arbitrary[ComparisonExpr[I, R]] =
    Arbitrary(for {
      i  <- getArbitrary[I]
      l  <- getArbitrary[ValueExpression[I, R]]
      op <- getArbitrary[Comparison]
      r  <- getArbitrary[ValueExpression[I, R]]
    } yield ComparisonExpr(i, l, op, r))

  implicit def arbQComparison[I: Arbitrary, R: Arbitrary]: Arbitrary[QuantifiedComparison[I, R]] =
    Arbitrary(for {
      i  <- getArbitrary[I]
      v  <- getArbitrary[ValueExpression[I, R]]
      c  <- getArbitrary[Comparison]
      qt <- getArbitrary[Quantifier]
      qr <- getArbitrary[Query[I, R]]
    } yield QuantifiedComparison(i, v, c, qt, qr))

  implicit def arbQuantifier: Arbitrary[Quantifier] =
    Arbitrary(Gen.oneOf(Gen.const(ALLQ), Gen.const(SOME), Gen.const(ANY)))

  implicit def arbBetween[I: Arbitrary, R: Arbitrary]: Arbitrary[Between[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      v <- getArbitrary[ValueExpression[I, R]]
      l <- getArbitrary[ValueExpression[I, R]]
      u <- getArbitrary[ValueExpression[I, R]]
    } yield Between(i, v, l, u))

  implicit def arbInList[I: Arbitrary, R: Arbitrary]: Arbitrary[InList[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      v <- getArbitrary[ValueExpression[I, R]]
      l <- Gen.resize(1, getArbitrary[List[Expression[I, R]]])
    } yield InList(i, v, l))

  implicit def arbInSubQuery[I: Arbitrary, R: Arbitrary]: Arbitrary[InSubQuery[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      v <- getArbitrary[ValueExpression[I, R]]
      q <- getArbitrary[Query[I, R]]
    } yield InSubQuery(i, v, q))

  implicit def arbLike[I: Arbitrary, R: Arbitrary]: Arbitrary[Like[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      v <- getArbitrary[ValueExpression[I, R]]
      p <- getArbitrary[ValueExpression[I, R]]
      e <- Gen.frequency((9, Gen.const(None)), (1, Gen.some(getArbitrary[ValueExpression[I, R]])))
    } yield Like(i, v, p, e))

  implicit def arbNullPredicate[I: Arbitrary, R: Arbitrary]: Arbitrary[NullPredicate[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      v <- getArbitrary[ValueExpression[I, R]]
    } yield NullPredicate(i, v))

  implicit def arbDistinctFrom[I: Arbitrary, R: Arbitrary]: Arbitrary[DistinctFrom[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      v <- getArbitrary[ValueExpression[I, R]]
      r <- getArbitrary[ValueExpression[I, R]]
    } yield DistinctFrom(i, v, r))

  implicit def arbValueExpression[I: Arbitrary, R: Arbitrary]: Arbitrary[ValueExpression[I, R]] =
    Arbitrary(
      Gen.frequency(
        (5, getArbitrary[PrimaryExpression[I, R]]),
        (5, getArbitrary[ArithmeticUnary[I, R]]),
        (1, getArbitrary[ArithmeticBinary[I, R]])
      )
    )

  implicit def arbArithmeticUnary[I: Arbitrary, R: Arbitrary]: Arbitrary[ArithmeticUnary[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      s <- getArbitrary[Sign]
      v <- getArbitrary[ValueExpression[I, R]]
    } yield ArithmeticUnary(i, s, v))

  implicit def arbArithmeticBinary[I: Arbitrary, R: Arbitrary]: Arbitrary[ArithmeticBinary[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      l <- getArbitrary[ValueExpression[I, R]]
      o <- getArbitrary[ArithmeticOperator]
      r <- getArbitrary[ValueExpression[I, R]]
    } yield ArithmeticBinary(i, l, o, r))

  implicit def arbArithmeticOperator: Arbitrary[ArithmeticOperator] =
    Arbitrary(
      Gen.oneOf(
        Gen.const(ADD),
        Gen.const(SUBTRACT),
        Gen.const(MULTIPLY),
        Gen.const(DIVIDE),
        Gen.const(MODULUS)
      )
    )

  implicit def arbPrimaryExpression[I: Arbitrary, R: Arbitrary]
      : Arbitrary[PrimaryExpression[I, R]] =
    Arbitrary(
      Gen.frequency(
        (50, getArbitrary[LiteralExpr[I, R]]),
        (50, getArbitrary[ColumnExpr[I, R]]),
        (1, getArbitrary[SubQueryExpr[I, R]]),
        (1, getArbitrary[ExistsExpr[I, R]]),
        (2, getArbitrary[SimpleCase[I, R]]),
        (2, getArbitrary[SearchedCase[I, R]]),
        (3, getArbitrary[Cast[I, R]]),
        (3, getArbitrary[Subscript[I, R]]),
        (3, getArbitrary[DereferenceExpr[I, R]]),
        (2, getArbitrary[Row[I, R]]),
        (2, getArbitrary[FunctionCall[I, R]]),
        (2, getArbitrary[IntervalLiteral[I, R]]),
        (2, getArbitrary[Extract[I, R]]),
        (20, getArbitrary[SpecialDateTimeFunc[I, R]])
      )
    )

  implicit def arbColumnExpr[I: Arbitrary, R: Arbitrary]: Arbitrary[ColumnExpr[I, R]] =
    Arbitrary(
      for { i <- getArbitrary[I]; v <- getArbitrary[ColumnRef[I, R]] } yield ColumnExpr(i, v)
    )

  implicit def arbSubQueryExpr[I: Arbitrary, R: Arbitrary]: Arbitrary[SubQueryExpr[I, R]] =
    Arbitrary(for { i <- getArbitrary[I]; v <- getArbitrary[Query[I, R]] } yield SubQueryExpr(i, v))

  implicit def arbExistsExpr[I: Arbitrary, R: Arbitrary]: Arbitrary[ExistsExpr[I, R]] =
    Arbitrary(for { i <- getArbitrary[I]; v <- getArbitrary[Query[I, R]] } yield ExistsExpr(i, v))

  implicit def arbSimpleCase[I: Arbitrary, R: Arbitrary]: Arbitrary[SimpleCase[I, R]] =
    Arbitrary(for {
      i  <- getArbitrary[I]
      e  <- getArbitrary[ValueExpression[I, R]]
      w  <- Gen.resize(2, getArbitrary[List[WhenClause[I, R]]])
      el <- Gen.some(getArbitrary[Expression[I, R]])
    } yield SimpleCase(i, e, w, el))

  implicit def arbSearchedCase[I: Arbitrary, R: Arbitrary]: Arbitrary[SearchedCase[I, R]] =
    Arbitrary(for {
      i  <- getArbitrary[I]
      w  <- Gen.resize(2, getArbitrary[List[WhenClause[I, R]]])
      el <- Gen.some(getArbitrary[Expression[I, R]])
    } yield SearchedCase(i, w, el))

  implicit def arbWhenClause[I: Arbitrary, R: Arbitrary]: Arbitrary[WhenClause[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      c <- getArbitrary[Expression[I, R]]
      r <- getArbitrary[Expression[I, R]]
    } yield WhenClause(i, c, r))

  implicit def arbCast[I: Arbitrary, R: Arbitrary]: Arbitrary[Cast[I, R]] =
    Arbitrary(for {
      i  <- getArbitrary[I]
      e  <- getArbitrary[Expression[I, R]]
      ty <- getArbitrary[String]
      tr <- getArbitrary[Boolean]
    } yield Cast(i, e, ty, tr))

  implicit def arbBooleanOperator: Arbitrary[BooleanOperator] =
    Arbitrary(Gen.oneOf(Gen.const(AND), Gen.const(OR)))

  implicit def arbComparison: Arbitrary[Comparison] =
    Arbitrary(
      Gen.oneOf(
        Gen.const(EQ),
        Gen.const(NEQ),
        Gen.const(LT),
        Gen.const(LTE),
        Gen.const(GT),
        Gen.const(GTE)
      )
    )

  implicit def arbSubscript[I: Arbitrary, R: Arbitrary]: Arbitrary[Subscript[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      v <- getArbitrary[PrimaryExpression[I, R]]
      n <- getArbitrary[ValueExpression[I, R]]
    } yield Subscript(i, v, n))

  implicit def arbDereferenceExpr[I: Arbitrary, R: Arbitrary]: Arbitrary[DereferenceExpr[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      b <- getArbitrary[PrimaryExpression[I, R]]
      f <- getArbitrary[String]
    } yield DereferenceExpr(i, b, f))

  implicit def arbLiteralExpr[I: Arbitrary, R: Arbitrary]: Arbitrary[LiteralExpr[I, R]] = {
    def nullLiteral =
      for { i <- getArbitrary[I] } yield NullLiteral(i).asInstanceOf[LiteralExpr[I, R]]
    def intLiteral =
      for { i <- getArbitrary[I]; v <- getArbitrary[Long] } yield IntLiteral(i, v)
        .asInstanceOf[LiteralExpr[I, R]]
    def decimalLiteral =
      for { i <- getArbitrary[I]; v <- getArbitrary[Double] } yield DecimalLiteral(i, v)
        .asInstanceOf[LiteralExpr[I, R]]
    def doubleLiteral =
      for { i <- getArbitrary[I]; v <- getArbitrary[Double] } yield DoubleLiteral(i, v)
        .asInstanceOf[LiteralExpr[I, R]]
    def stringLiteral =
      for { i <- getArbitrary[I]; v <- getArbitrary[String] } yield StringLiteral(i, v)
        .asInstanceOf[LiteralExpr[I, R]]
    def booleanLiteral =
      for { i <- getArbitrary[I]; v <- getArbitrary[Boolean] } yield BooleanLiteral(i, v)
        .asInstanceOf[LiteralExpr[I, R]]
    Arbitrary(
      Gen
        .oneOf(
          nullLiteral,
          intLiteral,
          decimalLiteral,
          doubleLiteral,
          stringLiteral,
          booleanLiteral
        )
    )
  }

  implicit def arbRow[I: Arbitrary, R: Arbitrary]: Arbitrary[Row[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      e <- Gen.resize(3, getArbitrary[List[Expression[I, R]]])
    } yield Row(i, e))

  implicit def arbFunctionCall[I: Arbitrary, R: Arbitrary]: Arbitrary[FunctionCall[I, R]] =
    Arbitrary(for {
      i  <- getArbitrary[I]
      n  <- getArbitrary[String]
      s  <- Gen.option(getArbitrary[SetQuantifier])
      e  <- Gen.resize(1, getArbitrary[List[Expression[I, R]]])
      or <- Gen.frequency((9, Gen.const(None)), (1, Gen.some(getArbitrary[OrderBy[I, R]])))
      f  <- Gen.frequency((9, Gen.const(None)), (1, Gen.some(getArbitrary[FunctionFilter[I, R]])))
      ov <- Gen.frequency((9, Gen.const(None)), (1, Gen.some(getArbitrary[FunctionOver[I, R]])))
    } yield FunctionCall(i, n, s, e, or, f, ov))

  implicit def arbFunctionFilter[I: Arbitrary, R: Arbitrary]: Arbitrary[FunctionFilter[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      e <- getArbitrary[LogicalBinary[I, R]]
    } yield FunctionFilter(i, e))

  implicit def arbFunctionOver[I: Arbitrary, R: Arbitrary]: Arbitrary[FunctionOver[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      e <- Gen.resize(1, getArbitrary[List[Expression[I, R]]])
      o <- Gen.frequency((9, Gen.const(None)), (1, Gen.some(getArbitrary[OrderBy[I, R]])))
      w <- Gen.frequency((9, Gen.const(None)), (1, Gen.some(getArbitrary[WindowFrame[I, R]])))
    } yield FunctionOver(i, e, o, w))

  implicit def arbWindowFrame[I: Arbitrary, R: Arbitrary]: Arbitrary[WindowFrame[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      f <- getArbitrary[FrameType]
      s <- getArbitrary[FrameBound[I, R]]
      e <- Gen.frequency((9, Gen.const(None)), (1, Gen.some(getArbitrary[FrameBound[I, R]])))
    } yield WindowFrame(i, f, s, e))

  implicit def arbFrameBound[I: Arbitrary, R: Arbitrary]: Arbitrary[FrameBound[I, R]] =
    Arbitrary(
      Gen.frequency(
        (5, getArbitrary[UnboundedFrame[I, R]]),
        (2, getArbitrary[CurrentRowBound[I, R]]),
        (1, getArbitrary[BoundedFrame[I, R]])
      )
    )

  implicit def arbUnboundedFrame[I: Arbitrary, R: Arbitrary]: Arbitrary[UnboundedFrame[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      b <- getArbitrary[BoundType]
    } yield UnboundedFrame(i, b))

  implicit def arbCurrentRowBound[I: Arbitrary, R: Arbitrary]: Arbitrary[CurrentRowBound[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
    } yield CurrentRowBound(i))

  implicit def arbBoundedFrame[I: Arbitrary, R: Arbitrary]: Arbitrary[BoundedFrame[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      b <- getArbitrary[BoundType]
      e <- getArbitrary[Expression[I, R]]
    } yield BoundedFrame(i, b, e))

  implicit def arbBoundType: Arbitrary[BoundType] =
    Arbitrary(Gen.oneOf(Gen.const(PRECEDING), Gen.const(FOLLOWING)))

  implicit def arbFrameType: Arbitrary[FrameType] =
    Arbitrary(Gen.oneOf(Gen.const(RANGE), Gen.const(ROWS)))

  implicit def arbIntervalLiteral[I: Arbitrary, R: Arbitrary]: Arbitrary[IntervalLiteral[I, R]] = {
    def stringLiteral =
      for { i <- getArbitrary[I]; v <- getArbitrary[String] } yield StringLiteral(i, v)
        .asInstanceOf[StringLiteral[I, R]]
    Arbitrary(for {
      i <- getArbitrary[I]
      s <- getArbitrary[Sign]
      v <- stringLiteral
      f <- getArbitrary[IntervalField]
      t <- Gen.option(getArbitrary[IntervalField])
    } yield IntervalLiteral(i, s, v, f, t))
  }

  implicit def arbSign: Arbitrary[Sign] =
    Arbitrary(Gen.oneOf(Gen.const(PLUS), Gen.const(MINUS)))

  implicit def arbIntervalField: Arbitrary[IntervalField] =
    Arbitrary(
      Gen.oneOf(
        Gen.const(YEAR),
        Gen.const(MONTH),
        Gen.const(DAY),
        Gen.const(HOUR),
        Gen.const(MINUTE),
        Gen.const(SECOND)
      )
    )

  implicit def arbExtract[I: Arbitrary, R: Arbitrary]: Arbitrary[Extract[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      f <- getArbitrary[String]
      e <- getArbitrary[ValueExpression[I, R]]
    } yield Extract(i, f, e))

  implicit def arbSpecialDateTimeFunc[I: Arbitrary, R: Arbitrary]
      : Arbitrary[SpecialDateTimeFunc[I, R]] = {
    Arbitrary(for {
      i <- getArbitrary[I]
      n <- getArbitrary[CurrentTime]
      p <- Gen.option(getArbitrary[Int])
    } yield SpecialDateTimeFunc(i, n, p))
  }

  implicit def arbCurrentTime: Arbitrary[CurrentTime] =
    Arbitrary(
      Gen.oneOf(
        Gen.const(CURRENT_DATE),
        Gen.const(CURRENT_TIME),
        Gen.const(CURRENT_TIMESTAMP),
        Gen.const(LOCALTIME),
        Gen.const(LOCALTIMESTAMP)
      )
    )

}
