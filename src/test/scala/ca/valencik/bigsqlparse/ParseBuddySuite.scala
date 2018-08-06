package ca.valencik.bigsqlparse

import org.scalatest._
import ca.valencik.bigsqlparse.ParseBuddy.catalog

class ParseBuddySpec extends FlatSpec with Matchers {

  "ParseBuddy" should "parse valid SQL queries" in {
    val queries = List(
      "SELECT COUNT(1);",
      "SELECT name, COUNT(*) FROM bar;",
      "SELECT name, COUNT(*) FROM bar WHERE bar.age >= 18;",
      "SELECT name, COUNT(*) FROM bar WHERE bar.age >= 18 GROUP BY name;",
      "SELECT name, COUNT(*) FROM bar WHERE bar.age >= 18 GROUP BY name HAVING COUNT(name) >= 2;",
      "SELECT name, COUNT(*) FROM bar WHERE bar.age >= 18 ORDER BY age LIMIT 2;",
      "select x from foo join bar on foo.a = bar.b",
      "select x from foo join bar as derp on foo.a = derp.a",
      "select x from foo join bar as derp (alias1, alias2) on foo.a = derp.a",
      "select * from foo f join bar b using (a);"
    )
    queries.map { case q => ParseBuddy.parse(q) }
  }

  it should "parse lower case queries" in {
    val queries = List(
      "select count(1);",
      "select name, count(*) from bar;"
    )
    queries.map { case q => ParseBuddy.parse(q) }
  }

  it should "parse queries without an ending ';'" in {
    val queries = List(
      "SELECT COUNT(1)",
      "SELECT NAME, COUNT(*) FROM BAR"
    )
    queries.map { case q => ParseBuddy.parse(q) }
  }

  it should "resolve select item names and table names in relations" in {
    val query = "select a, x, y from foo join bar on foo.a = bar.a"
    ParseBuddy.resolve(ParseBuddy.parse(query).right.get) shouldBe
      Some(List(List("db.foo.a", "db.bar.x", "db.bar.y"), List("db.foo", "db.bar")))
  }

}
