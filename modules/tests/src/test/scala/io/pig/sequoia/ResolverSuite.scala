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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ResolverSpec extends AnyFlatSpec with Matchers {

  import io.pig.sequoia.MonadSqlState._
  import io.pig.sequoia._

  val catalog    = Catalog(Map("db" -> List("a")))
  val emptyState = Resolver()

  "Resolver" should "resolve ColumnRefs in scope" in {
    val initialState = emptyState.addRelationToScope("db", List("a"))
    val (log, finalState, rq) =
      resolveColumnRef(ColumnRef((), RawColumnName("a"))).value.run(catalog, initialState).value

    log.isEmpty shouldBe false
    finalState shouldBe initialState.copy(selectionS = List("a"))
    rq shouldBe Right(ColumnRef((), ResolvedColumnName("a")))
  }

  it should "not resolve ColumnRefs not in scope" in {
    val initialState = emptyState
    val (log, finalState, rq) =
      resolveColumnRef(ColumnRef((), RawColumnName("a"))).value.run(catalog, initialState).value

    log.isEmpty shouldBe false
    finalState shouldBe initialState
    rq shouldBe Left(ResolutionError(RawColumnName("a")))
  }

  it should "resolve TableRefs in the catalog" in {
    val initialState = emptyState
    val (log, finalState, rq) =
      resolveTableRef(TableRef((), RawTableName("db"))).value.run(catalog, initialState).value

    log.isEmpty shouldBe false
    finalState shouldBe initialState.addRelationToScope("db", List("a"))
    rq shouldBe Right(TableRef((), ResolvedTableName("db")))
  }

  it should "not resolve TableRefs not in the catalog" in {
    val emptyCat = Catalog(Map.empty)
    val (log, finalState, rq) =
      resolveTableRef(TableRef((), RawTableName("db"))).value.run(emptyCat, emptyState).value

    log.isEmpty shouldBe false
    finalState shouldBe emptyState
    rq shouldBe Left(ResolutionError(RawTableName("db")))
  }
}
