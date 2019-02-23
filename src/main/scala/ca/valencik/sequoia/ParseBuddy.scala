package ca.valencik.sequoia

import org.antlr.v4.runtime.{
  BaseErrorListener,
  CodePointCharStream,
  CharStream,
  CharStreams,
  CommonTokenStream,
  IntStream,
  RecognitionException,
  Recognizer
}
import org.antlr.v4.runtime.misc.Interval
import pprint.pprintln

object ParseBuddy {

  type Info = Int

  case class ParseFailure(error: String)

  case class AntlrParseException(msg: String) extends Exception(msg)

  case object ParseErrorListener extends BaseErrorListener {
    override def syntaxError(recognizer: Recognizer[_, _],
                             offendingSymbol: scala.Any,
                             line: Int,
                             charPositionInLine: Int,
                             msg: String,
                             e: RecognitionException): Unit = {
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

  val catalog = Resolver(Map("DB.FOO" -> Set("A", "B")))
}

object ParseBuddyApp extends App {
  import ca.valencik.sequoia.ParseBuddy._

  private val exitCommands                  = Seq("exit", ":q", "q")
  def exitCommand(command: String): Boolean = exitCommands.contains(command.toLowerCase)

  def inputLoop(): Unit = {
    val inputQuery = scala.io.StdIn.readLine("\nParseBuddy> ")
    if (!exitCommand(inputQuery)) {
      val pq = parse(inputQuery)
      pprintln(pq, height = 10000)
      inputLoop()
    }
  }

  inputLoop()
}

/**
  * This string stream provides the lexer with upper case characters only. This greatly simplifies
  * lexing the stream, while we can maintain the original command.
  *
  * This was taken from Spark's spark/sql/catalyst/parser/ParseDriver.scala
  *
  * The comment below (taken from the original class) describes the rationale for doing this:
  *
  * This class provides and implementation for a case insensitive token checker for the lexical
  * analysis part of antlr. By converting the token stream into upper case at the time when lexical
  * rules are checked, this class ensures that the lexical rules need to just match the token with
  * upper case letters as opposed to combination of upper case and lower case characters. This is
  * purely used for matching lexical rules. The actual token text is stored in the same way as the
  * user input without actually converting it into an upper case. The token values are generated by
  * the consume() function of the super class ANTLRStringStream. The LA() function is the lookahead
  * function and is purely used for matching lexical rules. This also means that the grammar will
  * only accept capitalized tokens in case it is run from other tools like antlrworks which do not
  * have the UpperCaseCharStream implementation.
  */
class UpperCaseCharStream(wrapped: CodePointCharStream) extends CharStream {
  override def consume(): Unit            = wrapped.consume
  override def getSourceName(): String    = wrapped.getSourceName
  override def index(): Int               = wrapped.index
  override def mark(): Int                = wrapped.mark
  override def release(marker: Int): Unit = wrapped.release(marker)
  override def seek(where: Int): Unit     = wrapped.seek(where)
  override def size(): Int                = wrapped.size

  override def getText(interval: Interval): String = {
    // ANTLR 4.7's CodePointCharStream implementations have bugs when
    // getText() is called with an empty stream, or intervals where
    // the start > end. See
    // https://github.com/antlr/antlr4/commit/ac9f7530 for one fix
    // that is not yet in a released ANTLR artifact.
    if (size() > 0 && (interval.b - interval.a >= 0)) {
      wrapped.getText(interval)
    } else {
      ""
    }
  }

  override def LA(i: Int): Int = {
    val la = wrapped.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
}
