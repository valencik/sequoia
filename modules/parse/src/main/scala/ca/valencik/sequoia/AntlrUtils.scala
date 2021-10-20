package ca.valencik.sequoia

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
