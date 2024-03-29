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

class ParseAndResolveSuite extends AnyFlatSpec with Matchers {

  import io.pig.sequoia.ParseBuddy
  import io.pig.sequoia.MonadSqlState._
  import io.pig.sequoia._

  val catalog    = Catalog(Map("db" -> List("a"), "db2" -> List("a2", "b2")))
  val emptyState = Resolver()

  "ParseAndResolver" should "resolve simple queries from catalog" in {
    val parsedQuery = ParseBuddy.parse("select a from db")
    val (log, finalState, rq) =
      parsedQuery.map(resolveQuery).toOption.get.value.run(catalog, emptyState).value

    log.isEmpty shouldBe false
    finalState shouldBe emptyState.addRelationToScope("db", List("a")).addColumnToProjection("a")
    rq.isRight shouldBe true
  }

  it should "resolve simple queries from catalog with alias" in {
    val parsedQuery = ParseBuddy.parse("select a apple from db")
    val (log, finalState, rq) =
      parsedQuery.map(resolveQuery).toOption.get.value.run(catalog, emptyState).value

    val expected = emptyState
      .addRelationToScope("db", List("a"))
      .addColumnToProjection("apple")
    log.isEmpty shouldBe false
    finalState shouldBe expected
    rq.isRight shouldBe true
  }

  it should "not resolve simple queries not from catalog" in {
    val parsedQuery = ParseBuddy.parse("select a from foo")
    val (log, finalState, rq) =
      parsedQuery.map(resolveQuery).toOption.get.value.run(catalog, emptyState).value

    log.isEmpty shouldBe false
    finalState shouldBe emptyState
    rq.isRight shouldBe false
  }

  it should "not resolve simple queries with columns not in relation" in {
    val parsedQuery = ParseBuddy.parse("select b from db")
    val (log, finalState, rq) =
      parsedQuery.map(resolveQuery).toOption.get.value.run(catalog, emptyState).value

    log.isEmpty shouldBe false
    finalState shouldBe emptyState.addRelationToScope("db", List("a"))
    rq.isRight shouldBe false
  }

  it should "not resolve simple queries with some columns not in relation" in {
    val parsedQuery = ParseBuddy.parse("select a, b from db")
    val (log, finalState, rq) =
      parsedQuery.map(resolveQuery).toOption.get.value.run(catalog, emptyState).value

    log.isEmpty shouldBe false
    finalState shouldBe emptyState.addRelationToScope("db", List("a")).addColumnToProjection("a")
    rq.isRight shouldBe false
  }

  it should "resolve queries with CTEs from catalog" in {
    val parsedQuery = ParseBuddy.parse("with justA as (select a from db) select a from justA")
    val (log, finalState, rq) =
      parsedQuery.map(resolveQuery).toOption.get.value.run(catalog, emptyState).value

    val expected = emptyState
      .addRelationToScope("db", List("a"))
      .addColumnToProjection("a")
      .addCTE("justA")
      .resetRelationScope()
      .addRelationToScope("justA", List("a"))
      .addColumnToProjection("a")
    log.isEmpty shouldBe false
    finalState shouldBe expected
    rq.isRight shouldBe true
  }

  it should "not resolve queries with CTEs not in catalog" in {
    val parsedQuery = ParseBuddy.parse("with justA as (select a, b from db) select a from justA")
    val (log, finalState, rq) =
      parsedQuery.map(resolveQuery).toOption.get.value.run(catalog, emptyState).value

    val expected = emptyState
      .addRelationToScope("db", List("a"))
      .addColumnToProjection("a")
    log.isEmpty shouldBe false
    finalState shouldBe expected
    rq.isRight shouldBe false
  }

  it should "not resolve queries with CTEs from catalog but with outer query out of scope" in {
    val parsedQuery = ParseBuddy.parse("with justA as (select a from db) select b from justA")
    val (log, finalState, rq) =
      parsedQuery.map(resolveQuery).toOption.get.value.run(catalog, emptyState).value

    val expected = emptyState
      .addRelationToScope("db", List("a"))
      .addColumnToProjection("a")
      .addCTE("justA")
      .resetRelationScope()
      .addRelationToScope("justA", List("a"))
    log.isEmpty shouldBe false
    finalState shouldBe expected
    rq.isRight shouldBe false
  }

  it should "resolve queries with CTEs from catalog and column aliases" in {
    val parsedQuery =
      ParseBuddy.parse("with justA as (select a apple from db) select apple from justA")
    val (log, finalState, rq) =
      parsedQuery.map(resolveQuery).toOption.get.value.run(catalog, emptyState).value

    val expected = emptyState
      .addRelationToScope("db", List("a"))
      .addColumnToProjection("a")
      .addColumnAlias("apple")
      .addCTE("justA")
      .resetRelationScope()
      .addRelationToScope("justA", List("apple"))
      .addColumnToProjection("apple")
    log.isEmpty shouldBe false
    finalState shouldBe expected
    rq.isRight shouldBe true
  }

  it should "not resolve queries with CTEs where the outer query does not use the proper column alias" in {
    val parsedQuery =
      ParseBuddy.parse("with justA as (select a apple from db) select a from justA")
    val (log, finalState, rq) =
      parsedQuery.map(resolveQuery).toOption.get.value.run(catalog, emptyState).value

    val expected = emptyState
      .addRelationToScope("db", List("a"))
      .addColumnToProjection("a")
      .addColumnAlias("apple")
      .addCTE("justA")
      .resetRelationScope()
      .addRelationToScope("justA", List("apple"))
    log.isEmpty shouldBe false
    finalState shouldBe expected
    rq.isRight shouldBe false
  }

  it should "resolve queries with CTEs from catalog, column aliases and a select all" in {
    val parsedQuery =
      ParseBuddy.parse("with justA as (select a apple from db) select * from justA")
    val (log, finalState, rq) =
      parsedQuery.map(resolveQuery).toOption.get.value.run(catalog, emptyState).value

    val expected = emptyState
      .addRelationToScope("db", List("a"))
      .addColumnToProjection("a")
      .addColumnAlias("apple")
      .addCTE("justA")
      .resetRelationScope()
      .addRelationToScope("justA", List("apple"))
      .addAllColumnsToProjection
    log.isEmpty shouldBe false
    finalState shouldBe expected
    rq.isRight shouldBe true
  }

  it should "resolve queries with CTEs from catalog, column aliases and a select all with table ref" in {
    val parsedQuery =
      ParseBuddy.parse("with justA as (select a apple from db) select justA.* from justA")
    val (log, finalState, rq) =
      parsedQuery.map(resolveQuery).toOption.get.value.run(catalog, emptyState).value

    val expected = emptyState
      .addRelationToScope("db", List("a"))
      .addColumnToProjection("a")
      .addColumnAlias("apple")
      .addCTE("justA")
      .resetRelationScope()
      .addRelationToScope("justA", List("apple"))
      .addAllColumnsFromRelationToProjection("justA")
    log.isEmpty shouldBe false
    finalState shouldBe expected
    rq.isRight shouldBe true
  }

  it should "resolve queries with CTEs from catalog, column aliases and a select all inside and out with table ref" in {
    val parsedQuery =
      ParseBuddy.parse("with justA as (select * from db) select justA.* from justA")
    val (log, finalState, rq) =
      parsedQuery.map(resolveQuery).toOption.get.value.run(catalog, emptyState).value

    val expected = emptyState
      .addRelationToScope("db", List("a"))
      .addAllColumnsToProjection
      .addCTE("justA")
      .resetRelationScope()
      .addRelationToScope("justA", List("a"))
      .addAllColumnsFromRelationToProjection("justA")
    log.isEmpty shouldBe false
    finalState shouldBe expected
    rq.isRight shouldBe true
  }

  it should "resolve queries with SubQueryExprs from catalog without leaking state" in {
    val parsedQuery =
      ParseBuddy.parse("select (select a from db)")
    val (log, finalState, rq) =
      parsedQuery.map(resolveQuery).toOption.get.value.run(catalog, emptyState).value

    val expected = emptyState
      .addColumnToProjection("_col0")
    log.isEmpty shouldBe false
    finalState shouldBe expected
    rq.isRight shouldBe true
  }

  it should "resolve queries with SubQueryExprs from catalog including CTEs without leaking state" in {
    val parsedQuery =
      ParseBuddy.parse(
        "select (with justA as (select a from db) select justA.* from justA) as subQA"
      )
    val (log, finalState, rq) =
      parsedQuery.map(resolveQuery).toOption.get.value.run(catalog, emptyState).value

    val expected = emptyState
      .addColumnToProjection("subQA")
    log.isEmpty shouldBe false
    finalState shouldBe expected
    rq.isRight shouldBe true
  }

  it should "resolve queries with anonymous columns" in {
    val parsedQuery =
      ParseBuddy.parse("select 0, 1")
    val (log, finalState, rq) =
      parsedQuery.map(resolveQuery).toOption.get.value.run(catalog, emptyState).value

    val expected = emptyState
      .addColumnToProjection("_col0")
      .addColumnToProjection("_col1")
    log.isEmpty shouldBe true // we never do any catalog lookups
    finalState shouldBe expected
    rq.isRight shouldBe true
  }

  it should "resolve queries with anonymous columns and aliases" in {
    val parsedQuery =
      ParseBuddy.parse("select 0, 1 as one")
    val (log, finalState, rq) =
      parsedQuery.map(resolveQuery).toOption.get.value.run(catalog, emptyState).value

    val expected = emptyState
      .addColumnToProjection("_col0")
      .addColumnToProjection("one")
    log.isEmpty shouldBe true // we never do any catalog lookups
    finalState shouldBe expected
    rq.isRight shouldBe true
  }

  it should "resolve queries with a mix of regular and anonymous columns" in {
    val parsedQuery =
      ParseBuddy.parse("select a, 1 from db")
    val (log, finalState, rq) =
      parsedQuery.map(resolveQuery).toOption.get.value.run(catalog, emptyState).value

    val expected = emptyState
      .addRelationToScope("db", List("a"))
      .addColumnToProjection("a")
      .addColumnToProjection("_col1")
    log.isEmpty shouldBe false
    finalState shouldBe expected
    rq.isRight shouldBe true
  }

  it should "resolve queries with sampled tables with sub query expressions" in {
    val parsedQuery =
      ParseBuddy.parse(
        "select a, 42 from db TABLESAMPLE BERNOULLI ((select a num from db limit 1))"
      )
    val (log, finalState, rq) =
      parsedQuery.map(resolveQuery).toOption.get.value.run(catalog, emptyState).value

    val expected = emptyState
      .addRelationToScope("db", List("a"))
      .addColumnToProjection("a")
      .addColumnToProjection("_col1")
    log.isEmpty shouldBe false
    finalState shouldBe expected
    rq.isRight shouldBe true
  }

  it should "resolve queries with join relations" in {
    val parsedQuery =
      ParseBuddy.parse(
        "select a, b2 from db join db2 on a = a2"
      )
    val (log, finalState, rq) =
      parsedQuery.map(resolveQuery).toOption.get.value.run(catalog, emptyState).value

    val expected = emptyState
      .addRelationToScope("db", List("a"))
      .addRelationToScope("db2", List("a2", "b2"))
      .addColumnToProjection("a")
      .addColumnToProjection("b2")
    log.isEmpty shouldBe false
    finalState shouldBe expected
    rq.isRight shouldBe true
  }
}
