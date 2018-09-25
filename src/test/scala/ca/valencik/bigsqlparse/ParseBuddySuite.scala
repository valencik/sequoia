package ca.valencik.bigsqlparse

import org.scalatest._
import ca.valencik.bigsqlparse.ParseBuddy._

import scala.collection.mutable.HashMap

class ParseBuddySpec extends FlatSpec with Matchers {

  "ParseBuddy" should "parse valid SQL queries" in {
    val queries = List(
      "SELECT COUNT(1);",
      "SELECT name, COUNT(*) FROM bar;",
      "SELECT name, COUNT(*) FROM bar WHERE bar.age >= 18;",
      "SELECT name, COUNT(*) FROM bar WHERE bar.age >= 18 AND bar.other == something;",
      "SELECT name, COUNT(*) FROM bar WHERE bar.age >= 18 GROUP BY name;",
      "SELECT name, COUNT(*) FROM bar WHERE bar.age >= 18 GROUP BY name HAVING COUNT(name) >= 2;",
      "SELECT name, COUNT(*) FROM bar WHERE bar.age >= 18 ORDER BY age LIMIT 2;",
      "select x from foo join bar on foo.a = bar.b",
      "select x from foo join bar as derp on foo.a = derp.a",
      "select x from foo join bar as derp (alias1, alias2) on foo.a = derp.a",
      "select * from foo f join bar b using (a);",
      "with everything as (select * from events limit 2) select year, month from everything where year > month and year > 1000"
    )
    queries.forall { parse(_).isRight } shouldBe true
  }

  it should "parse lower case queries" in {
    parse("select count(1);").isRight shouldBe true
    parse("select name, count(*) from bar;").isRight shouldBe true
  }

  it should "parse queries without an ending ';'" in {
    parse("select count(1)").isRight shouldBe true
    parse("SELECT NAME, COUNT(*) FROM BAR").isRight shouldBe true
  }

  it should "not produce nulls on bad input" in {
    parse("func() over () as thing").isLeft shouldBe true
    parse("select x from").isLeft shouldBe true
  }

  def catalog: Catalog =
    Catalog(
      HashMap(
        "db" -> HashMap(
          "foo" -> Seq("a", "b", "c"),
          "bar" -> Seq("x", "y", "z")
        )))

  "ParseBuddy Resolving QueryNoWith" should "resolve simple statements" in {
    val q        = parse("select a from db.foo").right.get
    val resolved = resolveRelations(catalog, q, None)
    resolved.queryNoWith.querySpecification.from.relations.get shouldBe List(Table(ResolvedRelation("db.foo")))
  }

  it should "resolve to public if the relation is not in the catalog" in {
    val q        = parse("select a from fake").right.get
    val resolved = resolveRelations(catalog, q, None)
    resolved.queryNoWith.querySpecification.from.relations.get shouldBe List(Table(ResolvedRelation("public.fake")))
  }

  "ParseBuddy Resolving QueryWith" should "resolve simple statements" in {
    val q        = parse("with f as (select a from db.foo) select a from f").right.get
    val resolved = resolveRelations(catalog, q, None)
    resolved.withz.get.queries(0).query.queryNoWith.querySpecification.from.relations.get shouldBe List(
      Table(ResolvedRelation("db.foo")))
    resolved.queryNoWith.querySpecification.from.relations.get shouldBe List(Table(ResolvedRelation("cteAlias.f")))
  }

  it should "resolve to public if the relation is not an alias" in {
    val q        = parse("with f as (select a from db.foo) select a from fake").right.get
    val resolved = resolveRelations(catalog, q, None)
    resolved.withz.get.queries(0).query.queryNoWith.querySpecification.from.relations.get shouldBe List(
      Table(ResolvedRelation("db.foo")))
    resolved.queryNoWith.querySpecification.from.relations.get shouldBe List(Table(ResolvedRelation("public.fake")))
  }

  it should "resolve even if the named query usese a public table?" in {
    val q        = parse("with f as (select a from fake) select a from f").right.get
    val resolved = resolveRelations(catalog, q, None)
    resolved.withz.get.queries(0).query.queryNoWith.querySpecification.from.relations.get shouldBe List(
      Table(ResolvedRelation("public.fake")))
    resolved.queryNoWith.querySpecification.from.relations.get shouldBe List(Table(ResolvedRelation("cteAlias.f")))
  }

  "ParseBuddy Resolving References in QueryNoWith" should "resolve simple statements" in {
    val acc      = catalog
    val q        = parse("select a from db.foo").right.get
    val resolved = resolveReferences(acc, resolveRelations(acc, q, None))
    resolved.queryNoWith.querySpecification.from.relations.get shouldBe List(Table(ResolvedRelation("db.foo")))
    resolved.queryNoWith.querySpecification.select.selectItems shouldBe List(
      SingleColumn(Identifier(ResolvedReference("db.foo.a")), None))
  }

  it should "resolve references to public if relation is not in catalog" in {
    val acc      = catalog
    val q        = parse("select a from fake").right.get
    val resolved = resolveReferences(acc, resolveRelations(acc, q, None))
    resolved.queryNoWith.querySpecification.from.relations.get shouldBe List(Table(ResolvedRelation("public.fake")))
    resolved.queryNoWith.querySpecification.select.selectItems shouldBe List(
      SingleColumn(Identifier(ResolvedReference("public.fake.a")), None))
  }

  "ParseBuddy Resolving References in QueryWith" should "resolve cte statements" in {
    val acc      = catalog
    val q        = parse("with f as (select a from db.foo) select a from f").right.get
    val resolved = resolveReferences(acc, resolveRelations(acc, q, None))
    resolved.withz.get.queries(0).query.queryNoWith.querySpecification.from.relations.get shouldBe List(
      Table(ResolvedRelation("db.foo")))
    resolved.queryNoWith.querySpecification.from.relations.get shouldBe List(Table(ResolvedRelation("cteAlias.f")))
    resolved.queryNoWith.querySpecification.select.selectItems shouldBe List(
      SingleColumn(Identifier(ResolvedReference("cteAlias.f.a")), None))
  }

  it should "resolve references to public if relation is not in catalog" in {
    val acc      = catalog
    val q        = parse("with f as (select a from fake) select a from f").right.get
    val resolved = resolveReferences(acc, resolveRelations(acc, q, None))
    resolved.withz.get.queries(0).query.queryNoWith.querySpecification.from.relations.get shouldBe List(
      Table(ResolvedRelation("public.fake")))
    resolved.queryNoWith.querySpecification.from.relations.get shouldBe List(Table(ResolvedRelation("cteAlias.f")))
    resolved.queryNoWith.querySpecification.select.selectItems shouldBe List(
      SingleColumn(Identifier(ResolvedReference("cteAlias.f.a")), None))
  }

  ignore should "not resolve references if reference is not in tempView from cte" in {
    val acc      = catalog
    val q        = parse("with f as (select a from fake) select b from f").right.get
    val resolved = resolveReferences(acc, resolveRelations(acc, q, None))
    resolved.withz.get.queries(0).query.queryNoWith.querySpecification.from.relations.get shouldBe List(
      Table(ResolvedRelation("public.fake")))
    resolved.queryNoWith.querySpecification.from.relations.get shouldBe List(Table(ResolvedRelation("cteAlias.f")))
    resolved.queryNoWith.querySpecification.select.selectItems shouldBe List(
      SingleColumn(Identifier(UnresolvedReference("b")), None))
    assert(
      resolved.queryNoWith.querySpecification.select.selectItems != List(
        SingleColumn(Identifier(ResolvedReference("cteAlias.f.b")), None)))
  }

}
