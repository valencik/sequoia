package ca.valencik.sequoia

import cats.{Functor, Traverse}
import cats.tests.CatsSuite
import ca.valencik.sequoia.arbitrary._
import org.scalacheck.Arbitrary.{arbitrary => getArbitrary}

import cats.laws.discipline.{FunctorTests, SerializableTests, TraverseTests}

class TreeLawTests extends CatsSuite {
  checkAll("TableRef[Int, Int] with Option",
           TraverseTests[TableRef[Int, ?]].traverse[Int, Int, Int, Set[Int], Option, Option])
  checkAll("Traverse[TableRef[Int, ?]]", SerializableTests.serializable(Traverse[TableRef[Int, ?]]))

  checkAll("ColumnRef[Int, Int] with Option",
           TraverseTests[ColumnRef[Int, ?]].traverse[Int, Int, Int, Set[Int], Option, Option])
  checkAll("Traverse[ColumnRef[Int, ?]]", SerializableTests.serializable(Traverse[ColumnRef[Int, ?]]))

  checkAll("Query[Int, ?]", FunctorTests[Query[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Query[Int, ?]]", SerializableTests.serializable(Functor[Query[Int, ?]]))

  checkAll("QueryWith[Int, ?]", FunctorTests[QueryWith[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[QueryWith[Int, ?]]", SerializableTests.serializable(Functor[QueryWith[Int, ?]]))

  checkAll("QuerySelect[Int, ?]", FunctorTests[QuerySelect[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[QuerySelect[Int, ?]]", SerializableTests.serializable(Functor[QuerySelect[Int, ?]]))

  checkAll("QueryLimit[Int, ?]", FunctorTests[QueryLimit[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[QueryLimit[Int, ?]]", SerializableTests.serializable(Functor[QueryLimit[Int, ?]]))

  checkAll("CTE[Int, ?]", FunctorTests[CTE[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[CTE[Int, ?]]", SerializableTests.serializable(Functor[CTE[Int, ?]]))

  checkAll("Select[Int, ?]", FunctorTests[Select[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Select[Int, ?]]", SerializableTests.serializable(Functor[Select[Int, ?]]))

  checkAll("SelectCols[Int, ?]", FunctorTests[SelectCols[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[SelectCols[Int, ?]]", SerializableTests.serializable(Functor[SelectCols[Int, ?]]))

  checkAll("Selection[Int, ?]", FunctorTests[Selection[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Selection[Int, ?]]", SerializableTests.serializable(Functor[Selection[Int, ?]]))

  checkAll("SelectStar[Int, ?]", FunctorTests[SelectStar[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[SelectStar[Int, ?]]", SerializableTests.serializable(Functor[SelectStar[Int, ?]]))

  checkAll("SelectExpr[Int, ?]", FunctorTests[SelectExpr[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[SelectExpr[Int, ?]]", SerializableTests.serializable(Functor[SelectExpr[Int, ?]]))

  checkAll("From[Int, ?]", FunctorTests[From[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[From[Int, ?]]", SerializableTests.serializable(Functor[From[Int, ?]]))

  checkAll("Tablish[Int, ?]", FunctorTests[Tablish[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Tablish[Int, ?]]", SerializableTests.serializable(Functor[Tablish[Int, ?]]))

  checkAll("TablishTable[Int, ?]", FunctorTests[TablishTable[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[TablishTable[Int, ?]]", SerializableTests.serializable(Functor[TablishTable[Int, ?]]))

  checkAll("TablishSubquery[Int, ?]", FunctorTests[TablishSubquery[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[TablishSubquery[Int, ?]]", SerializableTests.serializable(Functor[TablishSubquery[Int, ?]]))

  checkAll("TablishJoin[Int, ?]", FunctorTests[TablishJoin[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[TablishJoin[Int, ?]]", SerializableTests.serializable(Functor[TablishJoin[Int, ?]]))

  checkAll("JoinCriteria[Int, ?]", FunctorTests[JoinCriteria[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[JoinCriteria[Int, ?]]", SerializableTests.serializable(Functor[JoinCriteria[Int, ?]]))

  checkAll("Expression[Int, ?]", FunctorTests[Expression[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Expression[Int, ?]]", SerializableTests.serializable(Functor[Expression[Int, ?]]))

  checkAll("ConstantExpr[Int, ?]", FunctorTests[ConstantExpr[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[ConstantExpr[Int, ?]]", SerializableTests.serializable(Functor[ConstantExpr[Int, ?]]))

  checkAll("ColumnExpr[Int, Int] with Option",
           TraverseTests[ColumnExpr[Int, ?]].traverse[Int, Int, Int, Set[Int], Option, Option])
  checkAll("Traverse[ColumnExpr[Int, ?]]", SerializableTests.serializable(Traverse[ColumnExpr[Int, ?]]))

  checkAll("SubQueryExpr[Int, ?]", FunctorTests[SubQueryExpr[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[SubQueryExpr[Int, ?]]", SerializableTests.serializable(Functor[SubQueryExpr[Int, ?]]))
}
