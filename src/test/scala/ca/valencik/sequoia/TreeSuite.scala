package ca.valencik.sequoia

import cats._
import cats.tests.CatsSuite
import org.scalacheck.ScalacheckShapeless._

import cats.laws.discipline.{FunctorTests, SerializableTests, TraverseTests}

class TreeLawTests extends CatsSuite {
  implicit def eqTableRef[I: Eq, R: Eq]: Eq[TableRef[I, R]] = Eq.fromUniversalEquals
  checkAll("TableRef[Int, Int] with Option",
           TraverseTests[TableRef[Int, ?]].traverse[Int, Int, Int, Set[Int], Option, Option])
  checkAll("Traverse[TableRef[Int, ?]]", SerializableTests.serializable(Traverse[TableRef[Int, ?]]))

  implicit def eqColumnRef[I: Eq, R: Eq]: Eq[ColumnRef[I, R]] = Eq.fromUniversalEquals
  checkAll("ColumnRef[Int, Int] with Option",
           TraverseTests[ColumnRef[Int, ?]].traverse[Int, Int, Int, Set[Int], Option, Option])
  checkAll("Traverse[ColumnRef[Int, ?]]", SerializableTests.serializable(Traverse[ColumnRef[Int, ?]]))

  implicit def eqQuery[I: Eq, R: Eq]: Eq[Query[I, R]] = Eq.fromUniversalEquals
  checkAll("Query[Int, ?]", FunctorTests[Query[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Query[Int, ?]]", SerializableTests.serializable(Functor[Query[Int, ?]]))

  implicit def eqQueryWith[I: Eq, R: Eq]: Eq[QueryWith[I, R]] = Eq.fromUniversalEquals
  checkAll("QueryWith[Int, ?]", FunctorTests[QueryWith[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[QueryWith[Int, ?]]", SerializableTests.serializable(Functor[QueryWith[Int, ?]]))

  implicit def eqQuerySelect[I: Eq, R: Eq]: Eq[QuerySelect[I, R]] = Eq.fromUniversalEquals
  checkAll("QuerySelect[Int, ?]", FunctorTests[QuerySelect[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[QuerySelect[Int, ?]]", SerializableTests.serializable(Functor[QuerySelect[Int, ?]]))

  implicit def eqQueryLimit[I: Eq, R: Eq]: Eq[QueryLimit[I, R]] = Eq.fromUniversalEquals
  checkAll("QueryLimit[Int, ?]", FunctorTests[QueryLimit[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[QueryLimit[Int, ?]]", SerializableTests.serializable(Functor[QueryLimit[Int, ?]]))

  implicit def eqCTE[I: Eq, R: Eq]: Eq[CTE[I, R]] = Eq.fromUniversalEquals
  checkAll("CTE[Int, ?]", FunctorTests[CTE[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[CTE[Int, ?]]", SerializableTests.serializable(Functor[CTE[Int, ?]]))

  implicit def eqSelect[I: Eq, R: Eq]: Eq[Select[I, R]] = Eq.fromUniversalEquals
  checkAll("Select[Int, ?]", FunctorTests[Select[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Select[Int, ?]]", SerializableTests.serializable(Functor[Select[Int, ?]]))

  implicit def eqSelectCols[I: Eq, R: Eq]: Eq[SelectCols[I, R]] = Eq.fromUniversalEquals
  checkAll("SelectCols[Int, ?]", FunctorTests[SelectCols[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[SelectCols[Int, ?]]", SerializableTests.serializable(Functor[SelectCols[Int, ?]]))

  implicit def eqSelection[I: Eq, R: Eq]: Eq[Selection[I, R]] = Eq.fromUniversalEquals
  checkAll("Selection[Int, ?]", FunctorTests[Selection[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Selection[Int, ?]]", SerializableTests.serializable(Functor[Selection[Int, ?]]))

  implicit def eqSelectStar[I: Eq, R: Eq]: Eq[SelectStar[I, R]] = Eq.fromUniversalEquals
  checkAll("SelectStar[Int, ?]", FunctorTests[SelectStar[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[SelectStar[Int, ?]]", SerializableTests.serializable(Functor[SelectStar[Int, ?]]))

  implicit def eqSelectExpr[I: Eq, R: Eq]: Eq[SelectExpr[I, R]] = Eq.fromUniversalEquals
  checkAll("SelectExpr[Int, ?]", FunctorTests[SelectExpr[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[SelectExpr[Int, ?]]", SerializableTests.serializable(Functor[SelectExpr[Int, ?]]))

  implicit def eqFrom[I: Eq, R: Eq]: Eq[From[I, R]] = Eq.fromUniversalEquals
  checkAll("From[Int, ?]", FunctorTests[From[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[From[Int, ?]]", SerializableTests.serializable(Functor[From[Int, ?]]))

  implicit def eqTablish[I: Eq, R: Eq]: Eq[Tablish[I, R]] = Eq.fromUniversalEquals
  checkAll("Tablish[Int, ?]", FunctorTests[Tablish[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Tablish[Int, ?]]", SerializableTests.serializable(Functor[Tablish[Int, ?]]))

  implicit def eqTablishTable[I: Eq, R: Eq]: Eq[TablishTable[I, R]] = Eq.fromUniversalEquals
  checkAll("TablishTable[Int, ?]", FunctorTests[TablishTable[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[TablishTable[Int, ?]]", SerializableTests.serializable(Functor[TablishTable[Int, ?]]))

  implicit def eqTablishSubquery[I: Eq, R: Eq]: Eq[TablishSubquery[I, R]] = Eq.fromUniversalEquals
  checkAll("TablishSubquery[Int, ?]", FunctorTests[TablishSubquery[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[TablishSubquery[Int, ?]]", SerializableTests.serializable(Functor[TablishSubquery[Int, ?]]))

  implicit def eqExpression[I: Eq, R: Eq]: Eq[Expression[I, R]] = Eq.fromUniversalEquals
  checkAll("Expression[Int, ?]", FunctorTests[Expression[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Expression[Int, ?]]", SerializableTests.serializable(Functor[Expression[Int, ?]]))

  implicit def eqConstantExpr[I: Eq, R: Eq]: Eq[ConstantExpr[I, R]] = Eq.fromUniversalEquals
  checkAll("ConstantExpr[Int, ?]", FunctorTests[ConstantExpr[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[ConstantExpr[Int, ?]]", SerializableTests.serializable(Functor[ConstantExpr[Int, ?]]))

  implicit def eqColumnExpr[I: Eq, R: Eq]: Eq[ColumnExpr[I, R]] = Eq.fromUniversalEquals
  checkAll("ColumnExpr[Int, Int] with Option",
           TraverseTests[ColumnExpr[Int, ?]].traverse[Int, Int, Int, Set[Int], Option, Option])
  checkAll("Traverse[ColumnExpr[Int, ?]]", SerializableTests.serializable(Traverse[ColumnExpr[Int, ?]]))

  implicit def eqSubQueryExpr[I: Eq, R: Eq]: Eq[SubQueryExpr[I, R]] = Eq.fromUniversalEquals
  checkAll("SubQueryExpr[Int, ?]", FunctorTests[SubQueryExpr[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[SubQueryExpr[Int, ?]]", SerializableTests.serializable(Functor[SubQueryExpr[Int, ?]]))

}
