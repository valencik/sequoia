package ca.valencik.sequoia

import scala.collection.mutable.HashMap
import org.antlr.v4.runtime.{BaseErrorListener, CharStreams, CommonTokenStream, RecognitionException, Recognizer}

sealed trait ResolvableRelation {
  val value: String
}
case class ResolvedRelation(value: String)   extends ResolvableRelation
case class UnresolvedRelation(value: String) extends ResolvableRelation

sealed trait ResolvableReference
case class ResolvedReference(value: String)   extends ResolvableReference
case class UnresolvedReference(value: String) extends ResolvableReference

object ParseBuddy {

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

  def parse(input: String): Either[ParseFailure, Query[QualifiedName, String]] = {
    val charStream = CharStreams.fromString(input.toUpperCase)
    val lexer      = new SqlBaseLexer(charStream)
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokens = new CommonTokenStream(lexer)
    val parser = new SqlBaseParser(tokens)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)
    val prestoVisitor = new PrestoSqlVisitorApp()

    try {
      val node: Node = prestoVisitor.visit(parser.statement)
      val qnw        = node.asInstanceOf[Query[QualifiedName, String]]
      if (qnw == null) Left(ParseFailure("oops")) else Right(qnw)
    } catch {
      case e: AntlrParseException => Left(ParseFailure(e.msg))
    }
  }

  val catalog = Catalog(
    HashMap(
      "db" -> HashMap(
        "foo" -> Seq("a", "b", "c"),
        "bar" -> Seq("x", "y", "z")
      )))


}

object ParseBuddyApp extends App {
  import ca.valencik.sequoia.ParseBuddy._

  private val exitCommands                  = Seq("exit", ":q", "q")
  def exitCommand(command: String): Boolean = exitCommands.contains(command.toLowerCase)

  def inputLoop(): Unit = {
    val inputQuery = scala.io.StdIn.readLine("\nParseBuddy> ")
    if (!exitCommand(inputQuery)) {
      val q = parse(inputQuery)
      println(s"\n(main) Parse: $q \n")
      inputLoop()
    }
  }

  inputLoop()
}
