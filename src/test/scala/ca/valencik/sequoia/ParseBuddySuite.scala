package ca.valencik.sequoia

import org.scalatest._
import ca.valencik.sequoia.ParseBuddy._

class ParseBuddySpec extends FlatSpec with Matchers {

  "ParseBuddy" should "parse valid SQL queries" in {
    val queries = List(
      "SELECT COUNT(1)",
      "SELECT name, COUNT(*) FROM bar",
      "SELECT DISTINCT name, COUNT(*) FROM bar",
      "SELECT name, COUNT(*) FROM bar WHERE bar.age >= 18",
      "SELECT name, COUNT(*) FROM bar WHERE bar.age >= 18 AND bar.other = something",
      "SELECT name, COUNT(*) FROM bar WHERE bar.age >= 18 GROUP BY name",
      "SELECT name, COUNT(*) FROM bar WHERE bar.age >= 18 GROUP BY DISTINCT name",
      "SELECT name, COUNT(*) FROM bar WHERE bar.age >= 18 GROUP BY name HAVING COUNT(name) >= 2",
      "SELECT name, COUNT(*) FROM bar WHERE bar.age >= 18 ORDER BY age LIMIT 2",
      "SELECT name, COUNT(*) FROM bar ORDER BY age LIMIT 2",
      "select x from foo join bar on foo.a = bar.b",
      "select x from foo join bar as derp on foo.a = derp.a",
      "select x from foo join bar as derp (alias1, alias2) on foo.a = derp.a",
      "select * from foo f join bar b using (a)",
      "with everything as (select * from events limit 2) select year, month from everything where year > month and year > 1000",
      "with everything as (select * from events limit 2) select year, month from everything"
    )
    queries.foreach { q =>
      assert(parse(q).isRight)
    }
  }

  it should "parse lower case queries" in {
    parse("select count(1)").isRight shouldBe true
    parse("select name, count(*) from bar").isRight shouldBe true
  }

  it should "not produce nulls on bad input" in {
    parse("func() over () as thing").isLeft shouldBe true
    parse("select x from").isLeft shouldBe true
    parse("select a aa b bb from foo").isLeft shouldBe true
  }
}
