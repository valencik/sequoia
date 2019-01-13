package ca.valencik.sequoia

import org.scalatest._
import ca.valencik.sequoia.ParseBuddy._

class ParseBuddySpec extends FlatSpec with Matchers {

  def catalog = Resolver(Map("DB.FOO" -> Set("A", "B", "C"), "DB.BAR" -> Set("X", "Y", "Z")))

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
}
