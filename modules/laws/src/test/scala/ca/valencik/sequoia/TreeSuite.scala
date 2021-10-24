package ca.valencik.sequoia

import cats.implicits._
import cats.{Functor, Traverse}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.Checkers
import org.typelevel.discipline.scalatest.FunSuiteDiscipline
import cats.laws.discipline.{FunctorTests, SerializableTests, TraverseTests}

import ca.valencik.sequoia.arbitrary._

class TableRefLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "TableRef[Int, Int] with Option",
    TraverseTests[TableRef[Int, *]].traverse[Int, Int, Int, Set[Int], Option, Option]
  )
  checkAll("Traverse[TableRef[Int, *]]", SerializableTests.serializable(Traverse[TableRef[Int, *]]))
}

class ColumnRefLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "ColumnRef[Int, Int] with Option",
    TraverseTests[ColumnRef[Int, *]].traverse[Int, Int, Int, Set[Int], Option, Option]
  )
  checkAll(
    "Traverse[ColumnRef[Int, *]]",
    SerializableTests.serializable(Traverse[ColumnRef[Int, *]])
  )
}

class UsingColumnLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "UsingColumn[Int, Int] with Option",
    TraverseTests[UsingColumn[Int, *]].traverse[Int, Int, Int, Set[Int], Option, Option]
  )
  checkAll(
    "Traverse[UsingColumn[Int, *]]",
    SerializableTests.serializable(Traverse[UsingColumn[Int, *]])
  )

}
