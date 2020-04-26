import ca.valencik.sequoia.ParseBuddy
import ca.valencik.sequoia.Pretty
import ca.valencik.sequoia.Rewrite
import ca.valencik.sequoia.RawName

object TrivialCTE {

  import Pretty._
  import Rewrite._

  def main(args: Array[String]): Unit = {

    val queryString = "select apple, banana from foo where inventory >= 2 and x < 42 and largename = 666 and y != 27 limit 100"

    val ifFoo         = ifTableName[RawName] { r => r.value == "foo" }
    val ifQueryHasFoo = ifRelation(ifFoo)

    val qD = ParseBuddy
      .parse(queryString)
      .map { pq =>
        if (ifQueryHasFoo(pq)) {
          println("Rewriting query...")
          val rewrittenQ = setCTE(pq, "myCTE")
          prettyQuery(rewrittenQ).render(80)
        } else {
          println("Not rewritting query...")
          prettyQuery(pq).render(80)
        }
      }
      .getOrElse("Parse Failure")

    println(qD)
  }

}
