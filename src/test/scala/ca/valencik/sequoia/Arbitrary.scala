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

  implicit def arbColumnAlias[I: Arbitrary]: Arbitrary[ColumnAlias[I]] =
    Arbitrary(for { i <- getArbitrary[I]; s <- getArbitrary[String] } yield ColumnAlias(i, s))

  implicit def arbQuery[I: Arbitrary, R: Arbitrary]: Arbitrary[Query[I, R]] =
    Arbitrary(
      Gen.frequency((1, getArbitrary[QueryWith[I, R]]),
                    (10, getArbitrary[QuerySelect[I, R]]),
                    (4, getArbitrary[QueryLimit[I, R]])))

  implicit def arbQueryWith[I: Arbitrary, R: Arbitrary]: Arbitrary[QueryWith[I, R]] =
    Arbitrary(
      for { i <- getArbitrary[I]; c <- getArbitrary[List[CTE[I, R]]]; q <- getArbitrary[Query[I, R]] } yield
        QueryWith(i, c, q))

  implicit def arbQuerySelect[I: Arbitrary, R: Arbitrary]: Arbitrary[QuerySelect[I, R]] =
    Arbitrary(for { i <- getArbitrary[I]; s <- getArbitrary[Select[I, R]] } yield QuerySelect(i, s))

  implicit def arbQueryLimit[I: Arbitrary, R: Arbitrary]: Arbitrary[QueryLimit[I, R]] =
    Arbitrary(
      for { i <- getArbitrary[I]; l <- getArbitrary[Limit[I]]; s <- getArbitrary[Select[I, R]] } yield
        QueryLimit(i, l, s))

  implicit def arbCTE[I: Arbitrary, R: Arbitrary]: Arbitrary[CTE[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]; a <- getArbitrary[TablishAliasT[I]]; c <- getArbitrary[List[ColumnAlias[I]]];
      q <- getArbitrary[Query[I, R]]
    } yield CTE(i, a, c, q))

  implicit def arbLimit[I: Arbitrary]: Arbitrary[Limit[I]] =
    Arbitrary(for { i <- getArbitrary[I]; v <- getArbitrary[String] } yield Limit(i, v))

  implicit def arbSelect[I: Arbitrary, R: Arbitrary]: Arbitrary[Select[I, R]] =
    Arbitrary(
      for { i <- getArbitrary[I]; s <- getArbitrary[SelectCols[I, R]]; f <- getArbitrary[Option[From[I, R]]] } yield
        Select(i, s, f))

  implicit def arbSelectCols[I: Arbitrary, R: Arbitrary]: Arbitrary[SelectCols[I, R]] =
    Arbitrary(for { i <- getArbitrary[I]; v <- getArbitrary[List[Selection[I, R]]] } yield SelectCols(i, v))

  implicit def arbSelection[I: Arbitrary, R: Arbitrary]: Arbitrary[Selection[I, R]] =
    Arbitrary(Gen.frequency((1, getArbitrary[SelectStar[I, R]]), (2, getArbitrary[SelectExpr[I, R]])))

  implicit def arbSelectStar[I: Arbitrary, R: Arbitrary]: Arbitrary[SelectStar[I, R]] =
    Arbitrary(for { i <- getArbitrary[I]; v <- getArbitrary[Option[TableRef[I, R]]] } yield SelectStar(i, v))

  implicit def arbSelectExpr[I: Arbitrary, R: Arbitrary]: Arbitrary[SelectExpr[I, R]] =
    Arbitrary(for {
      i <- getArbitrary[I]; v <- getArbitrary[Expression[I, R]]; a <- getArbitrary[Option[ColumnAlias[I]]]
    } yield SelectExpr(i, v, a))

  implicit def arbFrom[I: Arbitrary, R: Arbitrary]: Arbitrary[From[I, R]] =
    Arbitrary(for { i <- getArbitrary[I]; v <- getArbitrary[List[Tablish[I, R]]] } yield From(i, v))

  implicit def arbTablish[I: Arbitrary, R: Arbitrary]: Arbitrary[Tablish[I, R]] =
    Arbitrary(Gen.frequency((14, getArbitrary[TablishTable[I, R]]), (1, getArbitrary[TablishSubquery[I, R]])))

  implicit def arbTablishTable[I: Arbitrary, R: Arbitrary]: Arbitrary[TablishTable[I, R]] =
    Arbitrary(
      for { i <- getArbitrary[I]; ta <- getArbitrary[TablishAlias[I]]; v <- getArbitrary[TableRef[I, R]] } yield
        TablishTable(i, ta, v))

  implicit def arbTablishSubquery[I: Arbitrary, R: Arbitrary]: Arbitrary[TablishSubquery[I, R]] =
    Arbitrary(
      for { i <- getArbitrary[I]; ta <- getArbitrary[TablishAlias[I]]; v <- getArbitrary[Query[I, R]] } yield
        TablishSubquery(i, ta, v))

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

  implicit def arbConstant[I: Arbitrary]: Arbitrary[Constant[I]] = {
    def intConstant     = for { i <- getArbitrary[I]; v <- getArbitrary[Int] } yield IntConstant(i, v)
    def decimalConstant = for { i <- getArbitrary[I]; v <- getArbitrary[Double] } yield DecimalConstant(i, v)
    def doubleConstant  = for { i <- getArbitrary[I]; v <- getArbitrary[Double] } yield DoubleConstant(i, v)
    def stringConstant  = for { i <- getArbitrary[I]; v <- getArbitrary[String] } yield StringConstant(i, v)
    def boolConstant    = for { i <- getArbitrary[I]; v <- getArbitrary[Boolean] } yield BoolConstant(i, v)
    Arbitrary(Gen.oneOf(intConstant, decimalConstant, doubleConstant, stringConstant, boolConstant))
  }
}
