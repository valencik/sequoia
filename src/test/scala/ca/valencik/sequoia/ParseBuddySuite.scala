package ca.valencik.sequoia

import org.scalatest._
import org.scalatest.prop.PropertyChecks
import ca.valencik.sequoia.ParseBuddy._

class ParseBuddySpec extends FlatSpec with Matchers with PropertyChecks {

  "ParseBuddy" should "parse simple SQL queries" in {
    val queries = Table(
      "SELECT 1",
      "SELECT 1.2E4",
      "SELECT true",
      "SELECT 'hello'",
      "SELECT name FROM bar",
      "SELECT ALL name FROM bar",
      "SELECT DISTINCT name FROM bar",
      "SELECT COUNT(1)",
      "SELECT name, COUNT(*) FROM bar",
      "SELECT DISTINCT name, COUNT(*) FROM bar",
      """SELECT "two words" FROM bar""",
      """SELECT a FROM bar WHERE a <> 'text'""",
      "SELECT name FROM bar WHERE bar.age >= 18",
      "SELECT name FROM bar WHERE bar.age >= 18 AND bar.other = something",
      "SELECT name FROM bar WHERE bar.age >= 18 ORDER BY age LIMIT 2",
      "SELECT name FROM bar ORDER BY age LIMIT 2"
    )
    forAll(queries)(shouldParseWithNoNulls)
  }

  it should "parse queries with GROUP BY clauses" in {
    val queries = Table(
      "SELECT name FROM bar GROUP BY name",
      "SELECT name FROM bar GROUP BY ALL name",
      "SELECT name FROM bar GROUP BY DISTINCT name",
      "SELECT name FROM bar GROUP BY name HAVING name >= 2"
    )
    forAll(queries)(shouldParseWithNoNulls)
  }

  it should "parse queries with JOINs" in {
    val queries = Table(
      "select x from foo JOIN bar on foo.a = bar.b",
      "select x from foo JOIN bar as derp on foo.a = derp.a",
      "select x from foo JOIN bar as derp (alias1, alias2) on foo.a = derp.a",
      "select * from foo f JOIN bar b using (a)"
    )
    forAll(queries)(shouldParseWithNoNulls)
  }

  it should "parse CTEs" in {
    val queries = Table(
      "with everything as (select * from events limit 2) select year, month from everything where year > month and year > 1000",
      "with everything as (select * from events limit 2) select year, month from everything"
    )
    forAll(queries)(shouldParseWithNoNulls)
  }

  it should "parse set operations" in {
    val queries = Table(
      "select col from db UNION select col from db2",
      "select col from db UNION ALL select col from db2",
      "select col from db UNION DISTINCT select col from db2",
      "select col from db EXCEPT select col from db2",
      "select col from db EXCEPT ALL select col from db2",
      "select col from db EXCEPT DISTINCT select col from db2",
      "select col from db INTERSECT select col from db2",
      "select col from db INTERSECT ALL select col from db2",
      "select col from db INTERSECT DISTINCT select col from db2"
    )
    forAll(queries)(shouldParseWithNoNulls)
  }

  it should "parse SQL queries with function calls" in {
    val queries = Table(
      "SELECT COUNT(1)",
      "select count(DISTINCT name) from db.friends",
      """select count("Full name") over (partition by f."ID number") number_of_friends from db.friends f""",
      """select sum(f."Canadian") over (order by f.day rows 90 preceding) as "Canadian" from db.friends f"""
    )
    forAll(queries)(shouldParseWithNoNulls)
  }

  it should "be case insensitive for SQL keywords" in {
    parse("select 42") shouldBe parse("SELECT 42")
    parse("SELECT x FROM db.foo") shouldBe parse("select x from db.foo")
  }

  it should "fail to parse and return a Right on bad input" in {
    parse("func() over () as thing").isLeft shouldBe true
    parse("select x from").isLeft shouldBe true
    parse("select a aa b bb from foo").isLeft shouldBe true
  }

  it should "parse expression with null predicate" in {
    val queries = Table(
      "SELECT 1 IS NULL FROM bar",
      "SELECT 'a' IS NULL FROM bar"
    )
    forAll(queries)(shouldParseWithNoNulls)
  }

  it should "parse expression with not null predicate" in {
    val queries = Table(
      "SELECT 1 IS NOT NULL FROM bar",
      "SELECT 'a' IS NOT NULL FROM bar"
    )
    forAll(queries)(shouldParseWithNoNulls)
  }

  it should "parse interval expression" in {
    val queries = Table(
      "select INTERVAL '2019' YEAR",
      "select INTERVAL '12' MONTH",
      "select INTERVAL '1' DAY",
      "select INTERVAL +'10' HOUR",
      "select INTERVAL -'10' HOUR",
      "select INTERVAL '10' HOUR",
      "select INTERVAL '42:00' MINUTE TO SECOND",
      "select INTERVAL '42' SECOND",
      "select interval '1:01:30' hour to second - interval '1:01:29' hour to second"
    )
    forAll(queries)(shouldParseWithNoNulls)
  }
}
