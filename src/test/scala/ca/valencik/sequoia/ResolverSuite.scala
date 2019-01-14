package ca.valencik.sequoia

import org.scalatest._
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class ResolverSpec extends FlatSpec {

  def initialCatalog: Resolver = Resolver(Map("db.foo" -> Set("a", "b")))

  "Resolver" should "resolve columns in db in single Select" in {
    val q: QuerySelect[RawName, Int] = QuerySelect(
      1,
      Select(
        9,
        SelectCols(2, List(SelectExpr(5, ColumnExpr(3, ColumnRef(4, RawColumnName("a"))), None))),
        Some(From(8, List(TablishTable(7, TablishAliasNone(), TableRef(6, RawTableName("db.foo"))))))
      )
    )
    val actual = Resolver.resolveQuery(q).runA(initialCatalog).value
    val expected: QuerySelect[ResolvedName, Int] = QuerySelect(
      1,
      Select(
        9,
        SelectCols(2, List(SelectExpr(5, ColumnExpr(3, ColumnRef(4, ResolvedColumnName("a"))), None))),
        Some(From(8, List(TablishTable(7, TablishAliasNone(), TableRef(6, ResolvedTableName("db.foo"))))))
      )
    )
    actual shouldBe expected
  }

  it should "resolve columns in db inside CTE and columns from that CTE" in {
    val q: QueryWith[RawName, Int] = QueryWith(
      12,
      List(
        CTE(
          11,
          TablishAliasT(1, "hasA"),
          List(),
          QuerySelect(
            2,
            Select(
              10,
              SelectCols(3, List(SelectExpr(6, ColumnExpr(4, ColumnRef(5, RawColumnName("a"))), None))),
              Some(From(9, List(TablishTable(8, TablishAliasNone(), TableRef(7, RawTableName("db.foo"))))))
            )
          )
        )),
      QuerySelect(
        21,
        Select(
          20,
          SelectCols(13, List(SelectExpr(16, ColumnExpr(14, ColumnRef(15, RawColumnName("a"))), None))),
          Some(From(19, List(TablishTable(18, TablishAliasNone(), TableRef(17, RawTableName("hasA"))))))
        )
      )
    )
    val actual = Resolver.resolveQuery(q).runA(initialCatalog).value
    val expected: QueryWith[ResolvedName, Int] = QueryWith(
      12,
      List(
        CTE(
          11,
          TablishAliasT(1, "hasA"),
          List(),
          QuerySelect(
            2,
            Select(
              10,
              SelectCols(3, List(SelectExpr(6, ColumnExpr(4, ColumnRef(5, ResolvedColumnName("a"))), None))),
              Some(From(9, List(TablishTable(8, TablishAliasNone(), TableRef(7, ResolvedTableName("db.foo"))))))
            )
          )
        )),
      QuerySelect(
        21,
        Select(
          20,
          SelectCols(13, List(SelectExpr(16, ColumnExpr(14, ColumnRef(15, ResolvedColumnName("a"))), None))),
          Some(From(19, List(TablishTable(18, TablishAliasNone(), TableRef(17, ResolvedTableAlias("hasA"))))))
        )
      )
    )
    actual shouldBe expected
  }

  it should "not resolve columns in db but not inside CTE" in {
    val q: QueryWith[RawName, Int] = QueryWith(
      12,
      List(
        CTE(
          11,
          TablishAliasT(1, "hasA"),
          List(),
          QuerySelect(
            2,
            Select(
              10,
              SelectCols(3, List(SelectExpr(6, ColumnExpr(4, ColumnRef(5, RawColumnName("a"))), None))),
              Some(From(9, List(TablishTable(8, TablishAliasNone(), TableRef(7, RawTableName("db.foo"))))))
            )
          )
        )),
      QuerySelect(
        21,
        Select(
          20,
          SelectCols(13, List(SelectExpr(16, ColumnExpr(14, ColumnRef(15, RawColumnName("b"))), None))),
          Some(From(19, List(TablishTable(18, TablishAliasNone(), TableRef(17, RawTableName("hasA"))))))
        )
      )
    )
    val actual = Resolver.resolveQuery(q).runA(initialCatalog).value
    val expected: QueryWith[ResolvedName, Int] = QueryWith(
      12,
      List(
        CTE(
          11,
          TablishAliasT(1, "hasA"),
          List(),
          QuerySelect(
            2,
            Select(
              10,
              SelectCols(3, List(SelectExpr(6, ColumnExpr(4, ColumnRef(5, ResolvedColumnName("a"))), None))),
              Some(From(9, List(TablishTable(8, TablishAliasNone(), TableRef(7, ResolvedTableName("db.foo"))))))
            )
          )
        )),
      QuerySelect(
        21,
        Select(
          20,
          SelectCols(13, List(SelectExpr(16, ColumnExpr(14, ColumnRef(15, UnresolvedColumnName("b"))), None))),
          Some(From(19, List(TablishTable(18, TablishAliasNone(), TableRef(17, ResolvedTableAlias("hasA"))))))
        )
      )
    )
    actual shouldBe expected
  }

}
