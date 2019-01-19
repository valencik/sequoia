package ca.valencik.sequoia

import cats._
import cats.tests.CatsSuite
import org.scalacheck.ScalacheckShapeless._

import cats.laws.discipline.{FunctorTests, SerializableTests, TraverseTests}

class TreeLawTests extends CatsSuite {
  implicit def eqTableRef[I: Eq, R: Eq]: Eq[TableRef[I, R]]       = Eq.fromUniversalEquals
  implicit def eqColumnRef[I: Eq, R: Eq]: Eq[ColumnRef[I, R]]     = Eq.fromUniversalEquals
  implicit def eqColumnExpr[I: Eq, R: Eq]: Eq[ColumnExpr[I, R]]   = Eq.fromUniversalEquals
  implicit def eqQuery[I: Eq, R: Eq]: Eq[Query[I, R]]             = Eq.fromUniversalEquals
  implicit def eqQueryWith[I: Eq, R: Eq]: Eq[QueryWith[I, R]]     = Eq.fromUniversalEquals
  implicit def eqQuerySelect[I: Eq, R: Eq]: Eq[QuerySelect[I, R]] = Eq.fromUniversalEquals
  implicit def eqQueryLimit[I: Eq, R: Eq]: Eq[QueryLimit[I, R]]   = Eq.fromUniversalEquals

  checkAll("TableRef[Int, Int] with Option",
           TraverseTests[TableRef[Int, ?]].traverse[Int, Int, Int, Set[Int], Option, Option])
  checkAll("Traverse[TableRef[Int, ?]]", SerializableTests.serializable(Traverse[TableRef[Int, ?]]))

  checkAll("ColumnRef[Int, Int] with Option",
           TraverseTests[ColumnRef[Int, ?]].traverse[Int, Int, Int, Set[Int], Option, Option])
  checkAll("Traverse[ColumnRef[Int, ?]]", SerializableTests.serializable(Traverse[ColumnRef[Int, ?]]))

  checkAll("ColumnExpr[Int, Int] with Option",
           TraverseTests[ColumnExpr[Int, ?]].traverse[Int, Int, Int, Set[Int], Option, Option])
  checkAll("Traverse[ColumnExpr[Int, ?]]", SerializableTests.serializable(Traverse[ColumnExpr[Int, ?]]))

  checkAll("Query[Int, ?]", FunctorTests[Query[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Query[Int, ?]]", SerializableTests.serializable(Functor[Query[Int, ?]]))

  checkAll("QueryWith[Int, ?]", FunctorTests[QueryWith[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[QueryWith[Int, ?]]", SerializableTests.serializable(Functor[QueryWith[Int, ?]]))

  checkAll("QuerySelect[Int, ?]", FunctorTests[QuerySelect[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[QuerySelect[Int, ?]]", SerializableTests.serializable(Functor[QuerySelect[Int, ?]]))

  checkAll("QueryLimit[Int, ?]", FunctorTests[QueryLimit[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[QueryLimit[Int, ?]]", SerializableTests.serializable(Functor[QueryLimit[Int, ?]]))
}
