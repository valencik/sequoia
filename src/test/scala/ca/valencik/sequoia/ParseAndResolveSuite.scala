package ca.valencik.sequoia

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ParseAndResolveSuite extends AnyFlatSpec with Matchers {

  import ca.valencik.sequoia.ParseBuddy
  import ca.valencik.sequoia.MonadSqlState._
  import ca.valencik.sequoia._

  val catalog    = Catalog(Map("db" -> List("a")))
  val emptyState = Resolver()

  "ParseAndResolver" should "resolve simple queries from catalog" in {
    val parsedQuery = ParseBuddy.parse("select a from db")
    val (log, finalState, rq) =
      resolveQuery(parsedQuery.right.get).value.run(catalog, emptyState).value

    log.isEmpty shouldBe false
    finalState shouldBe emptyState.addRelationToScope("db", List("a")).addColumnToProjection("a")
    rq.isRight shouldBe true
  }

  it should "not resolve simple queries not from catalog" in {
    val parsedQuery = ParseBuddy.parse("select a from foo")
    val (log, finalState, rq) =
      resolveQuery(parsedQuery.right.get).value.run(catalog, emptyState).value

    log.isEmpty shouldBe false
    finalState shouldBe emptyState
    rq.isRight shouldBe false
  }

  it should "not resolve simple queries with columns not in relation" in {
    val parsedQuery = ParseBuddy.parse("select b from db")
    val (log, finalState, rq) =
      resolveQuery(parsedQuery.right.get).value.run(catalog, emptyState).value

    log.isEmpty shouldBe false
    finalState shouldBe emptyState.addRelationToScope("db", List("a"))
    rq.isRight shouldBe false
  }

  it should "not resolve simple queries with some columns not in relation" in {
    val parsedQuery = ParseBuddy.parse("select a, b from db")
    val (log, finalState, rq) =
      resolveQuery(parsedQuery.right.get).value.run(catalog, emptyState).value

    log.isEmpty shouldBe false
    finalState shouldBe emptyState.addRelationToScope("db", List("a")).addColumnToProjection("a")
    rq.isRight shouldBe false
  }

  it should "resolve queries with CTEs from catalog" in {
    val parsedQuery = ParseBuddy.parse("with justA as (select a from db) select a from justA")
    val (log, finalState, rq) =
      resolveQuery(parsedQuery.right.get).value.run(catalog, emptyState).value

    val expected = emptyState
      .addRelationToScope("db", List("a"))
      .addColumnToProjection("a")
      .addCTE("justA")
      .resetRelationScope
      .addRelationToScope("justA", List("a"))
      .addColumnToProjection("a")
    log.isEmpty shouldBe false
    finalState shouldBe expected
    rq.isRight shouldBe true
  }

  it should "not resolve queries with CTEs not in catalog" in {
    val parsedQuery = ParseBuddy.parse("with justA as (select a, b from db) select a from justA")
    val (log, finalState, rq) =
      resolveQuery(parsedQuery.right.get).value.run(catalog, emptyState).value

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
      resolveQuery(parsedQuery.right.get).value.run(catalog, emptyState).value

    val expected = emptyState
      .addRelationToScope("db", List("a"))
      .addColumnToProjection("a")
      .addCTE("justA")
      .resetRelationScope
      .addRelationToScope("justA", List("a"))
    log.isEmpty shouldBe false
    finalState shouldBe expected
    rq.isRight shouldBe false
  }

  it should "resolve queries with CTEs from catalog and column aliases" in {
    val parsedQuery =
      ParseBuddy.parse("with justA as (select a apple from db) select apple from justA")
    val (log, finalState, rq) =
      resolveQuery(parsedQuery.right.get).value.run(catalog, emptyState).value

    val expected = emptyState
      .addRelationToScope("db", List("a"))
      .addColumnToProjection("a")
      .aliasPreviousColumnInScope("apple")
      .addCTE("justA")
      .resetRelationScope
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
      resolveQuery(parsedQuery.right.get).value.run(catalog, emptyState).value

    val expected = emptyState
      .addRelationToScope("db", List("a"))
      .addColumnToProjection("a")
      .aliasPreviousColumnInScope("apple")
      .addCTE("justA")
      .resetRelationScope
      .addRelationToScope("justA", List("apple"))
    log.isEmpty shouldBe false
    finalState shouldBe expected
    rq.isRight shouldBe false
  }

  it should "resolve queries with CTEs from catalog, column aliases and a select all" in {
    val parsedQuery =
      ParseBuddy.parse("with justA as (select a apple from db) select * from justA")
    val (log, finalState, rq) =
      resolveQuery(parsedQuery.right.get).value.run(catalog, emptyState).value

    val expected = emptyState
      .addRelationToScope("db", List("a"))
      .addColumnToProjection("a")
      .aliasPreviousColumnInScope("apple")
      .addCTE("justA")
      .resetRelationScope
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
      resolveQuery(parsedQuery.right.get).value.run(catalog, emptyState).value

    val expected = emptyState
      .addRelationToScope("db", List("a"))
      .addColumnToProjection("a")
      .aliasPreviousColumnInScope("apple")
      .addCTE("justA")
      .resetRelationScope
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
      resolveQuery(parsedQuery.right.get).value.run(catalog, emptyState).value

    val expected = emptyState
      .addRelationToScope("db", List("a"))
      .addAllColumnsToProjection
      .addCTE("justA")
      .resetRelationScope
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
      resolveQuery(parsedQuery.right.get).value.run(catalog, emptyState).value

    val expected = emptyState
      .addColumnToProjection("_col0")
    log.isEmpty shouldBe false
    finalState shouldBe expected
    rq.isRight shouldBe true
  }

  it should "resolve queries with SubQueryExprs from catalog including CTEs without leaking state" in {
    val parsedQuery =
      ParseBuddy.parse("select (with justA as (select * from db) select justA.* from justA) as cteA")
    val (log, finalState, rq) =
      resolveQuery(parsedQuery.right.get).value.run(catalog, emptyState).value

    val expected = emptyState
      .addColumnToProjection("cteA")
    log.isEmpty shouldBe false
    finalState shouldBe expected
    rq.isRight shouldBe true
  }

  it should "resolve queries with anonymous columns" in {
    val parsedQuery =
      ParseBuddy.parse("select 0, 1")
    val (log, finalState, rq) =
      resolveQuery(parsedQuery.right.get).value.run(catalog, emptyState).value

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
      resolveQuery(parsedQuery.right.get).value.run(catalog, emptyState).value

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
      resolveQuery(parsedQuery.right.get).value.run(catalog, emptyState).value

    val expected = emptyState
      .addRelationToScope("db", List("a"))
      .addColumnToProjection("a")
      .addColumnToProjection("_col1")
    log.isEmpty shouldBe false
    finalState shouldBe expected
    rq.isRight shouldBe true
  }
}
