package ca.valencik.bigsqlparse

import org.antlr.v4.runtime._
import scala.util.Try

import scala.collection.JavaConversions._


sealed trait Expr
case class Expression(text:String) extends Expr

class PrestoSqlVisitorApp extends SqlBaseBaseVisitor[Expr] {

  override def visitSingleStatement(ctx: SqlBaseParser.SingleStatementContext) = Expression(ctx.getText)
  override def visitSingleExpression(ctx: SqlBaseParser.SingleExpressionContext) = Expression(ctx.getText)
  override def visitStatementDefault(ctx: SqlBaseParser.StatementDefaultContext) = Expression(ctx.getText)
  override def visitUnquotedIdentifier(ctx: SqlBaseParser.UnquotedIdentifierContext) = Expression(ctx.getText)

  override def visitQuery(ctx: SqlBaseParser.QueryContext) = {
    println("Called visitQuery")
    println("children size:", ctx.children.size)
    println("children iter:", ctx.children.iterator.map(_.getText).toList)
    visitQueryNoWith(ctx.queryNoWith)
  }

  override def visitQueryNoWith(ctx: SqlBaseParser.QueryNoWithContext) = {
    println("Called visitQueryNoWith")
    println("children size:", ctx.children.size)
    println("children iter:", ctx.children.iterator.map(_.getText).toList)
    visitQueryTerm(ctx.queryTerm)
  }

  def visitQueryTerm(ctx: SqlBaseParser.QueryTermContext) = {
    println("Called visitQueryTermDefault")
    println("children size:", ctx.children.size)
    println("children iter:", ctx.children.iterator.map(_.getText).toList)
    val qp = ctx.asInstanceOf[SqlBaseParser.QueryTermDefaultContext].queryPrimary
    visitQueryPrimary(qp)
  }

  def visitQueryPrimary(ctx: SqlBaseParser.QueryPrimaryContext) = {
    println("Called visitQueryPrimary")
    println("children size:", ctx.children.size)
    println("children iter:", ctx.children.iterator.map(_.getText).toList)
    val qs = ctx.asInstanceOf[SqlBaseParser.QueryPrimaryDefaultContext].querySpecification
    visitQuerySpecification(qs)
  }

  override def visitQuerySpecification(ctx: SqlBaseParser.QuerySpecificationContext) = {
    println("Called visitQuerySpecification")
    println("children size:", ctx.children.size)
    println("children iter:", ctx.children.iterator.map(_.getText).toList)
    println("children iter classes:", ctx.children.iterator.map(_.getClass.getName).toList)
    println("selectItem: ", ctx.selectItem.map(_.getText).toList)
    println("relations: ", ctx.relation.map(_.getText).toList)
    println("select: ", ctx.SELECT.getSymbol.getText)
    println("from: ", ctx.FROM.getSymbol.getText)
    println("where (null): ", ctx.WHERE)
    Expression(ctx.getText)
  }

}

object ParseBuddy {

  def parse(input: String): Expression = {
    val charStream = new ANTLRInputStream(input.toUpperCase)
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

  val exp: String            = "SELECT name, COUNT(*) FROM bar"
  val parsedExp: Expression  = parse(exp)
  println(exp, parsedExp)
}
