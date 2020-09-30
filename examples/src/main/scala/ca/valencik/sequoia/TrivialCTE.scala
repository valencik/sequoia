import ca.valencik.sequoia.ParseBuddy
import ca.valencik.sequoia.RawName
import ca.valencik.sequoia.Pretty
import ca.valencik.sequoia.Rewrite
import ca.valencik.sequoia.Lenses
import ca.valencik.sequoia.RawColumnName
import ca.valencik.sequoia.RawTableName

object TrivialCTE {

  import Pretty._
  import Rewrite.setCTE
  import Lenses.relationsFromQuery

  def main(args: Array[String]): Unit = {

    val queryString = """
    |with fruits as (
    |  select name, price
    |  from foo
    |  join bar on foo.name = bar.name
    |  where inventory >= 2 and x < 42 and largename = 666 and y != 27
    |)
    |select name, price from fruits
    |order by price
    |limit 100
    """.stripMargin

    val tableToFind   = "bar"
    def ifQueryHasFoo = relationsFromQuery[ParseBuddy.Info, RawName].exist(_.value == tableToFind)

    val qD = ParseBuddy
      .parse(queryString)
      .map { pq =>
        if (ifQueryHasFoo(pq)) {
          val tableNames = relationsFromQuery[ParseBuddy.Info, RawName].getAll(pq)
          tableNames.foreach {
            case r => {
              val msg = r match {
                case RawColumnName(value) => s"Found col: ${value}"
                case RawTableName(value)  => s"Found table: ${value}"
              }
              println(msg)
            }
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
