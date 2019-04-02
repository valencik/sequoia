package ca.valencik.sequoia

import org.scalatest._
import org.scalatest.prop.PropertyChecks
import ca.valencik.sequoia.ParseBuddy._

class ParseBuddySpec extends FlatSpec with Matchers with PropertyChecks {

  def noNulls(p: Product): Boolean = {
    p.productIterator.forall {
      case pp: Product => noNulls(pp)
      case x           => x != null
    }
  }

  "ParseBuddy" should "parse valid SQL queries" in {
    val queries = Table(
      "SELECT 1",
      "SELECT 1.2E4",
      "SELECT true",
      "SELECT 'hello'",
      "SELECT name FROM bar",
      "SELECT DISTINCT name FROM bar",
      "SELECT COUNT(1)",
      "SELECT name, COUNT(*) FROM bar",
      "SELECT DISTINCT name, COUNT(*) FROM bar",
      """SELECT "two words" FROM bar""",
      """SELECT a FROM bar WHERE a <> 'text'""",
      "SELECT name FROM bar WHERE bar.age >= 18",
      "SELECT name FROM bar WHERE bar.age >= 18 AND bar.other = something",
      "SELECT name FROM bar WHERE bar.age >= 18 GROUP BY name",
      "SELECT name FROM bar WHERE bar.age >= 18 GROUP BY DISTINCT name",
      "SELECT name FROM bar WHERE bar.age >= 18 GROUP BY name HAVING name >= 2",
      "SELECT name FROM bar WHERE bar.age >= 18 ORDER BY age LIMIT 2",
      "SELECT name FROM bar ORDER BY age LIMIT 2",
      "select x from foo join bar on foo.a = bar.b",
      "select x from foo join bar as derp on foo.a = derp.a",
      "select x from foo join bar as derp (alias1, alias2) on foo.a = derp.a",
      "select * from foo f join bar b using (a)",
      "with everything as (select * from events limit 2) select year, month from everything where year > month and year > 1000",
      "with everything as (select * from events limit 2) select year, month from everything"
    )
    forAll(queries) { q =>
      val pq = parse(q)
      assert(pq.isRight && pq.map(noNulls).getOrElse(false))
    }
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
    forAll(queries) { q =>
      val pq = parse(q)
      assert(pq.isRight && pq.map(noNulls).getOrElse(false))
    }
  }

  it should "parse SQL queries with function calls" in {
    val queries = Table(
      "SELECT COUNT(1)",
      "select count(DISTINCT name) from db.friends",
      """select count("Full name") over (partition by f."ID number") number_of_friends from db.friends f""",
      """select sum(f."Canadian") over (order by f.day rows 90 preceding) as "Canadian" from db.friends f"""
    )
    forAll(queries) { q =>
      val pq = parse(q)
      assert(pq.isRight && pq.map(noNulls).getOrElse(false))
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
