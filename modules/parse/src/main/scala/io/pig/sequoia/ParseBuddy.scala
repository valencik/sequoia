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

import org.antlr.v4.runtime.{
  BaseErrorListener,
  CharStreams,
  CommonTokenStream,
  RecognitionException,
  Recognizer
}
import pprint.pprintln

object ParseBuddy {

  type Info = Int

  case class ParseFailure(error: String)

  case class AntlrParseException(msg: String) extends Exception(msg)

  case object ParseErrorListener extends BaseErrorListener {
    override def syntaxError(
        recognizer: Recognizer[_, _],
        offendingSymbol: scala.Any,
        line: Int,
        charPositionInLine: Int,
        msg: String,
        e: RecognitionException
    ): Unit = {
      throw new AntlrParseException(msg)
    }
  }

  def parse(input: String): Either[ParseFailure, Query[Info, RawName]] = {
    val charStream = new UpperCaseCharStream(CharStreams.fromString(input))
    val lexer      = new SqlBaseLexer(charStream)
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokens = new CommonTokenStream(lexer)
    val parser = new SqlBaseParser(tokens)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)
    val prestoVisitor = new PrestoSqlVisitorApp()

    try {
      val node: Node = prestoVisitor.visit(parser.singleStatement)
      val qnw        = node.asInstanceOf[Query[Info, RawName]]
      if (qnw == null) Left(ParseFailure("Parser returned null")) else Right(qnw)
    } catch {
      case e: AntlrParseException => Left(ParseFailure(e.msg))
    }
  }

  def nullIndices(p: Product): Iterator[Int] =
    p.productIterator.zipWithIndex
      .filter { case (e, _) => e == null }
      .map { case (_, i) => i }

  def spotTheNulls(p: Product): Unit = {
    p.productIterator.foreach {
      case pp: Product => {
        nullIndices(pp).foreach { i =>
          println(f"Null at position ${i} in ${pp} which is inside ${p}")
        }
        spotTheNulls(pp)
      }
      case _ => ()
    }
  }

  def noNulls(p: Product): Boolean = {
    p.productIterator.forall {
      case pp: Product => noNulls(pp)
      case x           => x != null
    }
  }

  def shouldParseWithNoNulls(q: String): Unit = {
    val pq = parse(q)
    assert(pq.map(noNulls).getOrElse(false))
  }
}

object ParseBuddyApp {
  import io.pig.sequoia.ParseBuddy._
  import io.pig.sequoia.MonadSqlState._

  private val exitCommands                  = Seq("exit", ":q", "q")
  def exitCommand(command: String): Boolean = exitCommands.contains(command.toLowerCase)

  def resolveQandPrint(query: Query[Info, RawName]): Unit = {
    val catalog               = Catalog(Map("db" -> List("a", "b")))
    val emptyState            = Resolver()
    val (log, finalState, rq) = resolveQuery(query).value.run(catalog, emptyState).value
    pprintln(log.toList)
    pprintln(finalState)
    pprintln(rq)
  }

  def inputLoop(): Unit = {
    val inputQuery = scala.io.StdIn.readLine("\nParseBuddy> ")
    if (!exitCommand(inputQuery)) {
      val pq = parse(inputQuery)
      pprintln(pq, height = 10000)
      spotTheNulls(pq)

      pq.foreach(resolveQandPrint)
      inputLoop()
    }
  }

  def main(args: Array[String]): Unit = inputLoop()
}
