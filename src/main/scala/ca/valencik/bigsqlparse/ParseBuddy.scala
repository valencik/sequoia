package ca.valencik.bigsqlparse

import scala.collection.immutable.HashMap
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

object ParseBuddy {

  case class ParseFailure(error: String)

  def parse(input: String): Either[ParseFailure, QueryNoWith[QualifiedName]] = {
    val charStream = CharStreams.fromString(input.toUpperCase)
    val lexer      = new SqlBaseLexer(charStream)
    val tokens     = new CommonTokenStream(lexer)
    val parser     = new SqlBaseParser(tokens)

    val prestoVisitor = new PrestoSqlVisitorApp()
    val node: Node    = prestoVisitor.visit(parser.statement)
    val qnw           = node.asInstanceOf[QueryNoWith[QualifiedName]]
    if (qnw == null) Left(ParseFailure("oops")) else Right(qnw)
  }

  def mapRelation[A, B](f: A => B)(r: Relation[A]): Relation[B] = r match {
    case j: Join[A]             => Join(j.jointype, mapRelation(f)(j.left), mapRelation(f)(j.right), j.criterea)
    case sr: SampledRelation[A] => SampledRelation(mapRelation(f)(sr.relation), sr.sampleType, sr.samplePercentage)
    case ar: AliasedRelation[A] => AliasedRelation(mapRelation(f)(ar.relation), ar.alias, ar.columnNames)
    case t: Table[A]            => Table(f(t.name))
  }

  def relationToList[A](r: Relation[A]): Seq[A] = r match {
    case j: Join[A]             => relationToList(j.left) ++ relationToList(j.right)
    case sr: SampledRelation[A] => relationToList(sr.relation)
    case ar: AliasedRelation[A] => relationToList(ar.relation)
    case t: Table[A]            => Seq(t.name)
  }

  implicit val catalog = new Catalog(
    HashMap(
      "db" -> HashMap(
        "foo" -> Seq("a", "b", "c"),
        "bar" -> Seq("x", "y", "z")
      )))
  def resolve[R](q: QueryNoWith[R])(implicit schema: Catalog): Option[List[List[String]]] = {
    q.querySpecification.map { qs =>
      val ss: List[String] = qs.select.selectItems.flatMap {
        _ match {
          case s: SingleColumn => { s.expression match { case i: Identifier => Some(i.name); case _ => None } }
          case a: AllColumns   => a.name.map(_.name)
        }
      }
      val resolvedSelect = ss.flatMap(schema.nameColumn)
      val resolvedRelations: List[String] = qs.from.relations
        .map { relation =>
          // TODO specifying a function like rf should be considerably easier
          def rf(r: R): String = r match { case q: QualifiedName => q.name }
          mapRelation(rf)(relation)
        }
        .flatMap(relationToList)
        .flatMap(schema.nameTable)
      List(resolvedSelect, resolvedRelations)
    }
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
      println(s"Parse: $q")
      q.right.map(qnw => println(s"Resolved Columns: ${resolve(qnw)}"))
      inputLoop()
    }
  }

  inputLoop()
}
