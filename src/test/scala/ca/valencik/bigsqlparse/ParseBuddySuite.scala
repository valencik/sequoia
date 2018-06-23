package ca.valencik.bigsqlparse

import org.scalatest._

class ParseBuddySpec extends FlatSpec with Matchers {

  "ParseBuddy" should "parse valid SQL queries" in {
    val queries = List(
      "SELECT COUNT(1);",
      "SELECT name, COUNT(*) FROM bar;",
      "SELECT name, COUNT(*) FROM bar WHERE bar.age >= 18;",
      "SELECT name, COUNT(*) FROM bar WHERE bar.age >= 18 GROUP BY name;"
    )
    queries.map{case q => ParseBuddy.parse(q)}
  }

  it should "parse lower case queries" in {
    val queries = List(
      "select count(1);",
      "select name, count(*) from bar;"
    )
    queries.map{case q => ParseBuddy.parse(q)}
  }

  it should "parse queries without an ending ';'" in {
    val queries = List(
      "SELECT COUNT(1)",
      "SELECT NAME, COUNT(*) FROM BAR"
    )
    queries.map{case q => ParseBuddy.parse(q)}
  }

}
