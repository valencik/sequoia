/*
 * Copyright 2022 Pig.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pig.sequoia

import cats.implicits._
import cats.Traverse
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.Checkers
import org.typelevel.discipline.scalatest.FunSuiteDiscipline
import cats.laws.discipline.{SerializableTests, TraverseTests}

import io.pig.sequoia.arbitrary._

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
