package ca.valencik.bigsqlparse

import org.antlr.v4.runtime._
import scala.util.Try

import scala.collection.JavaConversions._


sealed trait Expr
case class Expression(text:String) extends Expr

class PrestoSqlVisitorApp extends SqlBaseBaseVisitor[Expr] {

  override def visitQuery(ctx: SqlBaseParser.QueryContext): Expression = {
    val exprText: String = ctx.getText
    Expression(exprText)
  }

}

object ParseBuddy {

  def parse(input: String): Expression = {
    val charStream = new ANTLRInputStream(input)
    val lexer      = new SqlBaseLexer(charStream)
    val tokens     = new CommonTokenStream(lexer)
    val parser     = new SqlBaseParser(tokens)

    val prestoVisitor = new PrestoSqlVisitorApp()
    val res           = prestoVisitor.visitQuery(parser.query)
    res.asInstanceOf[Expression]
  }

}

object ParseBuddyApp extends App {
  import ca.valencik.bigsqlparse.ParseBuddy._

  val exp: String            = "SELECT 1;"
  val parsedExp: Expression  = parse(exp)
  println(exp, parsedExp)
}
