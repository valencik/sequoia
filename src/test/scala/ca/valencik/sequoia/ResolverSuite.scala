package ca.valencik.sequoia

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ResolverSpec extends AnyFlatSpec with Matchers {

  import ca.valencik.sequoia.MonadSqlState._
  import ca.valencik.sequoia._

  val catalog    = Catalog(Map("db" -> List("a")))
  val emptyState = Resolver()

  "Resolver" should "resolve ColumnRefs in scope" in {
    val initialState = emptyState.addRelationToScope("db", List("a"))
    val (log, finalState, rq) =
      resolveColumnRef(ColumnRef((), RawColumnName("a"))).run(catalog, initialState).value

    log.isEmpty shouldBe false
    finalState shouldBe initialState.copy(s = List("a"))
    rq shouldBe Right(ColumnRef((), ResolvedColumnName("a")))
  }

  it should "not resolve ColumnRefs not in scope" in {
    val initialState = emptyState
    val (log, finalState, rq) =
      resolveColumnRef(ColumnRef((), RawColumnName("a"))).run(catalog, initialState).value

    log.isEmpty shouldBe false
    finalState shouldBe initialState
    rq shouldBe Left(ResolutionError(RawColumnName("a")))
  }

  it should "resolve TableRefs in the catalog" in {
    val initialState = emptyState
    val (log, finalState, rq) =
      resolveTableRef(TableRef((), RawTableName("db"))).run(catalog, initialState).value

    log.isEmpty shouldBe false
    finalState shouldBe initialState.addRelationToScope("db", List("a"))
    rq shouldBe Right(TableRef((), ResolvedTableName("db")))
  }

  it should "not resolve TableRefs not in the catalog" in {
    val emptyCat = Catalog(Map.empty)
    val (log, finalState, rq) =
      resolveTableRef(TableRef((), RawTableName("db"))).run(emptyCat, emptyState).value

    log.isEmpty shouldBe false
    finalState shouldBe emptyState
    rq shouldBe Left(ResolutionError(RawTableName("db")))
  }
}
