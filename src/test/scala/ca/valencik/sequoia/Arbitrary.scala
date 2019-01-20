package ca.valencik.sequoia

import cats._
import cats.tests.CatsSuite
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

  implicit def arbQuery[I: Arbitrary, R: Arbitrary]: Arbitrary[Query[I, R]] =
    Arbitrary(
      Gen.frequency((1, getArbitrary[QueryWith[I, R]]),
                    (10, getArbitrary[QuerySelect[I, R]]),
                    (4, getArbitrary[QueryLimit[I, R]])))

  implicit def arbQueryWith[I: Arbitrary, R: Arbitrary]: Arbitrary[QueryWith[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      c <- Gen.resize(5, getArbitrary[List[CTE[I, R]]])
      q <- getArbitrary[Query[I, R]]
    } yield QueryWith(i, c, q))

  implicit def arbQuerySelect[I: Arbitrary, R: Arbitrary]: Arbitrary[QuerySelect[I, R]] =
    Arbitrary(for { i <- getArbitrary[I]; s <- getArbitrary[Select[I, R]] } yield QuerySelect(i, s))

  implicit def arbQueryLimit[I: Arbitrary, R: Arbitrary]: Arbitrary[QueryLimit[I, R]] =
    Arbitrary(
      for { i <- getArbitrary[I]; l <- getArbitrary[Limit[I]]; s <- getArbitrary[Select[I, R]] } yield
        QueryLimit(i, l, s))

  implicit def arbCTE[I: Arbitrary, R: Arbitrary]: Arbitrary[CTE[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]; a <- getArbitrary[TablishAliasT[I]]
      c <- Gen.resize(5, getArbitrary[List[ColumnAlias[I]]])
      q <- getArbitrary[Query[I, R]]
    } yield CTE(i, a, c, q))

  implicit def arbLimit[I: Arbitrary]: Arbitrary[Limit[I]] =
    Arbitrary(for { i <- getArbitrary[I]; v <- getArbitrary[String] } yield Limit(i, v))

  implicit def arbSelect[I: Arbitrary, R: Arbitrary]: Arbitrary[Select[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      s <- getArbitrary[SelectCols[I, R]]
      f <- Gen.frequency((6, Gen.const(None)), (4, Gen.some(getArbitrary[From[I, R]])))
    } yield Select(i, s, f))

  implicit def arbSelectCols[I: Arbitrary, R: Arbitrary]: Arbitrary[SelectCols[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      v <- Gen.resize(5, getArbitrary[List[Selection[I, R]]])
    } yield SelectCols(i, v))

  implicit def arbSelection[I: Arbitrary, R: Arbitrary]: Arbitrary[Selection[I, R]] =
    Arbitrary(Gen.frequency((1, getArbitrary[SelectStar[I, R]]), (2, getArbitrary[SelectExpr[I, R]])))

  implicit def arbSelectStar[I: Arbitrary, R: Arbitrary]: Arbitrary[SelectStar[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      v <- Gen.frequency((9, Gen.const(None)), (1, Gen.some(getArbitrary[TableRef[I, R]])))
    } yield SelectStar(i, v))

  implicit def arbSelectExpr[I: Arbitrary, R: Arbitrary]: Arbitrary[SelectExpr[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      v <- getArbitrary[Expression[I, R]]
      a <- Gen.frequency((8, Gen.const(None)), (2, Gen.some(getArbitrary[ColumnAlias[I]])))
    } yield SelectExpr(i, v, a))

  implicit def arbFrom[I: Arbitrary, R: Arbitrary]: Arbitrary[From[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      v <- Gen.resize(3, getArbitrary[List[Tablish[I, R]]])
    } yield From(i, v))

  implicit def arbTablish[I: Arbitrary, R: Arbitrary]: Arbitrary[Tablish[I, R]] =
    Arbitrary(
      Gen.frequency((15, getArbitrary[TablishTable[I, R]]),
                    (4, getArbitrary[TablishJoin[I, R]]),
                    (1, getArbitrary[TablishSubquery[I, R]])))

  implicit def arbTablishTable[I: Arbitrary, R: Arbitrary]: Arbitrary[TablishTable[I, R]] =
    Arbitrary(
      for { i <- getArbitrary[I]; ta <- getArbitrary[TablishAlias[I]]; v <- getArbitrary[TableRef[I, R]] } yield
        TablishTable(i, ta, v))

  implicit def arbTablishSubquery[I: Arbitrary, R: Arbitrary]: Arbitrary[TablishSubquery[I, R]] =
    Arbitrary(
      for { i <- getArbitrary[I]; ta <- getArbitrary[TablishAlias[I]]; v <- getArbitrary[Query[I, R]] } yield
        TablishSubquery(i, ta, v))

  implicit def arbTablishJoin[I: Arbitrary, R: Arbitrary]: Arbitrary[TablishJoin[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]; jt <- getArbitrary[JoinType]; l <- getArbitrary[Tablish[I, R]]
      r <- getArbitrary[Tablish[I, R]]
      c <- Gen.frequency((9, Gen.const(None)), (1, Gen.some(getArbitrary[JoinCriteria[I, R]])))
    } yield TablishJoin(i, jt, l, r, c))

  implicit def arbJoinType: Arbitrary[JoinType] =
    Arbitrary(
      Gen.oneOf(Gen.const(LeftJoin),
                Gen.const(RightJoin),
                Gen.const(FullJoin),
                Gen.const(InnerJoin),
                Gen.const(CrossJoin)))

  implicit def arbJoinCriteria[I: Arbitrary, R: Arbitrary]: Arbitrary[JoinCriteria[I, R]] =
    Arbitrary(
      Gen.frequency((7, getArbitrary[NaturalJoin[I, R]]),
                    (3, getArbitrary[JoinOn[I, R]]),
                    (5, getArbitrary[JoinUsing[I, R]])))

  implicit def arbNaturalJoin[I: Arbitrary, R: Arbitrary]: Arbitrary[NaturalJoin[I, R]] =
    Arbitrary(for { i <- getArbitrary[I] } yield NaturalJoin(i))

  implicit def arbJoinOn[I: Arbitrary, R: Arbitrary]: Arbitrary[JoinOn[I, R]] =
    Arbitrary(for { i <- getArbitrary[I]; e <- getArbitrary[Expression[I, R]] } yield JoinOn(i, e))

  implicit def arbJoinUsing[I: Arbitrary, R: Arbitrary]: Arbitrary[JoinUsing[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]
      c <- Gen.resize(5, getArbitrary[List[UsingColumn[I, R]]])
    } yield JoinUsing(i, c))

  implicit def arbTablishAlias[I: Arbitrary]: Arbitrary[TablishAlias[I]] =
    Arbitrary(Gen.oneOf(Gen.const(TablishAliasNone[I]), getArbitrary[TablishAliasT[I]]))

  implicit def arbTablishAliasT[I: Arbitrary]: Arbitrary[TablishAliasT[I]] =
    Arbitrary(for { i <- getArbitrary[I]; v <- getArbitrary[String] } yield TablishAliasT(i, v))

  implicit def arbExpression[I: Arbitrary, R: Arbitrary]: Arbitrary[Expression[I, R]] =
    Arbitrary(
      Gen.frequency((5, getArbitrary[ConstantExpr[I, R]]),
                    (5, getArbitrary[ColumnExpr[I, R]]),
                    (1, getArbitrary[SubQueryExpr[I, R]])))

  implicit def arbConstantExpr[I: Arbitrary, R: Arbitrary]: Arbitrary[ConstantExpr[I, R]] =
    Arbitrary(for { i <- getArbitrary[I]; v <- getArbitrary[Constant[I]] } yield ConstantExpr(i, v))

  implicit def arbColumnExpr[I: Arbitrary, R: Arbitrary]: Arbitrary[ColumnExpr[I, R]] =
    Arbitrary(for { i <- getArbitrary[I]; v <- getArbitrary[ColumnRef[I, R]] } yield ColumnExpr(i, v))

  implicit def arbSubQueryExpr[I: Arbitrary, R: Arbitrary]: Arbitrary[SubQueryExpr[I, R]] =
    Arbitrary(for { i <- getArbitrary[I]; v <- getArbitrary[Query[I, R]] } yield SubQueryExpr(i, v))

  implicit def arbOperator: Arbitrary[Operator] =
    Arbitrary(Gen.oneOf(Gen.const(AND), Gen.const(OR)))

  implicit def arbComparison: Arbitrary[Comparison] =
    Arbitrary(Gen.oneOf(Gen.const(EQ), Gen.const(NEQ), Gen.const(LT), Gen.const(LTE), Gen.const(GT), Gen.const(GTE)))

  implicit def arbBooleanExpr[I: Arbitrary, R: Arbitrary]: Arbitrary[BooleanExpr[I, R]] =
    Arbitrary(for {
      i  <- getArbitrary[I]
      l  <- getArbitrary[Expression[I, R]]
      op <- getArbitrary[Operator]
      r  <- getArbitrary[Expression[I, R]]
    } yield BooleanExpr(i, l, op, r))

  implicit def arbComparisonExpr[I: Arbitrary, R: Arbitrary]: Arbitrary[ComparisonExpr[I, R]] =
    Arbitrary(for {
      i  <- getArbitrary[I]
      l  <- getArbitrary[Expression[I, R]]
      op <- getArbitrary[Comparison]
      r  <- getArbitrary[Expression[I, R]]
    } yield ComparisonExpr(i, l, op, r))

  implicit def arbConstant[I: Arbitrary]: Arbitrary[Constant[I]] = {
    def intConstant     = for { i <- getArbitrary[I]; v <- getArbitrary[Int] } yield IntConstant(i, v)
    def decimalConstant = for { i <- getArbitrary[I]; v <- getArbitrary[Double] } yield DecimalConstant(i, v)
    def doubleConstant  = for { i <- getArbitrary[I]; v <- getArbitrary[Double] } yield DoubleConstant(i, v)
    def stringConstant  = for { i <- getArbitrary[I]; v <- getArbitrary[String] } yield StringConstant(i, v)
    def boolConstant    = for { i <- getArbitrary[I]; v <- getArbitrary[Boolean] } yield BoolConstant(i, v)
    Arbitrary(Gen.oneOf(intConstant, decimalConstant, doubleConstant, stringConstant, boolConstant))
  }
}
