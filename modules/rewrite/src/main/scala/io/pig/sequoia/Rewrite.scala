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

package io.pig.sequoia

import pprint.pprintln

object Rewrite {

  def setCTE(q: Query[Int, RawName], name: String): Query[Int, RawName] = {
    val selectItems = q.queryNoWith.queryTerm match {
      case qs: QuerySpecification[Int, RawName] => qs.selectItems
      case _                                    => ???
    }
    val orderBy      = q.queryNoWith.orderBy
    val limit        = q.queryNoWith.limit
    val rewrittenQNW = q.queryNoWith.copy(orderBy = None, limit = None)
    val rewrittenQ   = q.copy(queryNoWith = rewrittenQNW)
    Query(
      14,
      Some(With(13, List(NamedQuery(12, name, None, rewrittenQ)))),
      QueryNoWith(
        23,
        QuerySpecification(
          info = 22,
          setQuantifier = None,
          selectItems = selectItems,
          from = List(
            SampledRelation(
              21,
              AliasedRelation(20, TableName(19, TableRef(18, RawTableName(name))), None, None),
              None
            )
          ),
          where = None,
          groupBy = None,
          having = None
        ),
        orderBy,
        limit
      )
    )
  }
}

object RewriteApp {
  import Rewrite._
  val inQ: Query[Int, RawName] = Query(
    1,
    None,
    QueryNoWith(
      10,
      QuerySpecification(
        9,
        None,
        List(SelectSingle(4, ColumnExpr(2, ColumnRef(3, RawColumnName("a"))), None)),
        List(
          SampledRelation(
            8,
            AliasedRelation(7, TableName(6, TableRef(5, RawTableName("foo"))), None, None),
            None
          )
        ),
        None,
        None,
        None
      ),
      None,
      None
    )
  )

  def main(args: Array[String]): Unit =
    pprintln(setCTE(inQ, "myCTE"), height = 10000)

}
