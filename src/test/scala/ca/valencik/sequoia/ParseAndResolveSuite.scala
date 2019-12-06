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
}
