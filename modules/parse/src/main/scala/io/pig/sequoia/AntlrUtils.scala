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

import org.antlr.v4.runtime.{CharStream, IntStream}
import org.antlr.v4.runtime.misc.Interval

/** This string stream provides the lexer with upper case characters only. This greatly simplifies
  * lexing the stream, while we can maintain the original command.
  *
  * Originally taken from Spark's spark/sql/catalyst/parser/ParseDriver.scala But now antlr
  * documents this in antlr4/doc/case-insensitive-lexing.md
  */
class UpperCaseCharStream(cs: CharStream) extends CharStream {
  override def consume(): Unit              = cs.consume
  override def getSourceName(): String      = cs.getSourceName
  override def index(): Int                 = cs.index
  override def mark(): Int                  = cs.mark
  override def release(marker: Int): Unit   = cs.release(marker)
  override def seek(where: Int): Unit       = cs.seek(where)
  override def size(): Int                  = cs.size
  override def getText(i: Interval): String = cs.getText(i)

  override def LA(i: Int): Int = {
    val la = cs.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
}
