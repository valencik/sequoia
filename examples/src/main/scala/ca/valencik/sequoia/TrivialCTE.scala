import ca.valencik.sequoia.ParseBuddy
import ca.valencik.sequoia.Pretty
import ca.valencik.sequoia.Rewrite
import ca.valencik.sequoia.Optics

//import pprint.pprintln

object TrivialCTE {

  import Pretty._
  import Rewrite.setCTE

  def main(args: Array[String]): Unit = {

    val queryString = """
    |with fruits as (
    |  select name, price
    |  from foo
    |  join bar on foo.name = bar.name
    |  where inventory >= jfkld
    |)
    |select name, price from fruits
    |order by price
    |limit 100
    """.stripMargin

    val tableToFind      = "bar"
    def ifQueryHasFoo[I] = Optics.tableNamesFromQuery[I].exist(_.value == tableToFind)

    val qD = ParseBuddy
      .parse(queryString)
      .map { pq =>
        if (ifQueryHasFoo(pq)) {
          val tableNames = Optics.tableNamesFromQuery[ParseBuddy.Info].getAll(pq)
          val colNames   = Optics.columnNamesFromQuery[ParseBuddy.Info].getAll(pq)
          // pprintln(pq, height = 10000)
          tableNames.foreach { case r =>
            println(s"Found table ${r.value}")
          }
          colNames.foreach { case r =>
            println(s"Found col ${r.value}")
          }
          println(s"Found table '${tableToFind}', rewriting query...")
          val rewrittenQ = setCTE(pq, "myCTE")
          prettyQuery(rewrittenQ).render(80)
        } else {
          println(s"Did not find table '${tableToFind}', skipping...")
          prettyQuery(pq).render(80)
        }
      }
      .getOrElse("Parse Failure")

    println(qD)
  }

}
