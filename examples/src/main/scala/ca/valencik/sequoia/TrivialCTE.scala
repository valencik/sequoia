import ca.valencik.sequoia.ParseBuddy
import ca.valencik.sequoia.RawName
import ca.valencik.sequoia.Pretty
import ca.valencik.sequoia.Rewrite
import ca.valencik.sequoia.Lenses

object TrivialCTE {

  import Pretty._
  import Rewrite.{setCTE, ifRelation}
  import Lenses.relationNames

  def main(args: Array[String]): Unit = {

    val queryString = """
    |select apple, banana
    |from foo
    |join bar on foo.a = bar.a
    |where inventory >= 2 and x < 42 and largename = 666 and y != 27
    |order by apple
    |limit 100
    """.stripMargin

    def ifFoo = relationNames[ParseBuddy.Info, RawName].exist(_.value.startsWith("bar"))
    val ifQueryHasFoo = ifRelation(ifFoo)

    val qD = ParseBuddy
      .parse(queryString)
      .map { pq =>
        if (ifQueryHasFoo(pq)) {
          println("Rewriting query...")
          val rewrittenQ = setCTE(pq, "myCTE")
          prettyQuery(rewrittenQ).render(30)
        } else {
          println("Not rewritting query...")
          prettyQuery(pq).render(80)
        }
      }
      .getOrElse("Parse Failure")

    println(qD)
  }

}
