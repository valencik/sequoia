/*
 * Copyright 2022 Pig.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import io.pig.sequoia.ParseBuddy
import io.pig.sequoia.Pretty
import io.pig.sequoia.Rewrite
import io.pig.sequoia.Optics

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
