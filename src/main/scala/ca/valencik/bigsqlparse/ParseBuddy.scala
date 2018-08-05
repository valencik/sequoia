package ca.valencik.bigsqlparse

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

object ParseBuddy {

  case class ParseFailure(error: String)

  def parse(input: String): Either[ParseFailure, QueryNoWith] = {
    val charStream = CharStreams.fromString(input.toUpperCase)
    val lexer      = new SqlBaseLexer(charStream)
    val tokens     = new CommonTokenStream(lexer)
    val parser     = new SqlBaseParser(tokens)

    val prestoVisitor = new PrestoSqlVisitorApp()
    val node: Node    = prestoVisitor.visit(parser.statement)
    val qnw           = node.asInstanceOf[QueryNoWith]
    if (qnw == null) Left(ParseFailure("oops")) else Right(qnw)
  }

  def analyze(q: QueryNoWith): Option[List[String]] = {
    q.querySpecification.map { qs =>
      qs.select.selectItems.flatMap(fSelect)
    }
  }

  def fSelect(si: SelectItem): List[String] = si match {
    case si: SingleColumn => fExpression(si.expression)
    case ac: AllColumns   => List(ac.name.map(_.name).getOrElse("*"))
  }

  def fExpression(e: Expression): List[String] = e match {
    case i: Identifier            => List(i.name)
    case q: QualifiedName         => List(q.name)
    case be: BooleanExpression    => fExpression(be.left) ++ fExpression(be.right)
    case ce: ComparisonExpression => fExpression(ce.left) ++ fExpression(ce.right)
    case np: IsNullPredicate      => fExpression(np.value)
    case nnp: IsNotNullPredicate  => fExpression(nnp.value)
  }

}

object ParseBuddyApp extends App {
  import ca.valencik.bigsqlparse.ParseBuddy._

  private val exitCommands                  = Seq("exit", ":q", "q")
  def exitCommand(command: String): Boolean = exitCommands.contains(command.toLowerCase)

  def inputLoop(): Unit = {
    val inputQuery = scala.io.StdIn.readLine("ParseBuddy> ")
    if (!exitCommand(inputQuery)) {
      val q = parse(inputQuery)
      println("Parse: ", q)
      q.right.map(qnw => println("Analyze: ", analyze(qnw)))
      inputLoop()
    }
  }

  inputLoop()
}
