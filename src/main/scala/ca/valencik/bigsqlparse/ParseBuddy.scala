package ca.valencik.bigsqlparse

import scala.collection.immutable.HashMap
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

object ParseBuddy {

  case class ParseFailure(error: String)

  def parse(input: String): Either[ParseFailure, QueryNoWith[QualifiedName, String]] = {
    val charStream = CharStreams.fromString(input.toUpperCase)
    val lexer      = new SqlBaseLexer(charStream)
    val tokens     = new CommonTokenStream(lexer)
    val parser     = new SqlBaseParser(tokens)

    val prestoVisitor = new PrestoSqlVisitorApp()
    val node: Node    = prestoVisitor.visit(parser.statement)
    val qnw           = node.asInstanceOf[QueryNoWith[QualifiedName, String]]
    if (qnw == null) Left(ParseFailure("oops")) else Right(qnw)
  }

  def mapExpression[A, B](f: A => B)(e: Expression[A]): Expression[B] = e match {
    case i: Identifier[A]         => Identifier(f(i.name))
    case be: BooleanExpression[A] => BooleanExpression(mapExpression(f)(be.left), be.op, mapExpression(f)(be.right))
    case ce: ComparisonExpression[A] =>
      ComparisonExpression(mapExpression(f)(ce.left), ce.op, mapExpression(f)(ce.right))
    case inp: IsNullPredicate[A]    => IsNullPredicate(mapExpression(f)(inp.value))
    case inn: IsNotNullPredicate[A] => IsNotNullPredicate(mapExpression(f)(inn.value))
  }

  // How do I get that joincriteria information out?
  def mapRelation[A, B](f: A => B)(r: Relation[A]): Relation[B] = r match {
    case j: Join[A]                => Join(j.jointype, mapRelation(f)(j.left), mapRelation(f)(j.right), j.criterea)
    case sr: SampledRelation[A, _] => SampledRelation(mapRelation(f)(sr.relation), sr.sampleType, sr.samplePercentage)
    case ar: AliasedRelation[A, _] => AliasedRelation(mapRelation(f)(ar.relation), ar.alias, ar.columnNames)
    case t: Table[A]               => Table(f(t.name))
  }

  //def mapJoin[A, B](f: Expression[A] => B)(r: Relation[_]) = r match {
  //  case j: Join[_] => j.criterea.map{jc => jc match {
  //    case jo: JoinOn[A] => jo.map(f)
  //  }}
  //  case _          => None
  //}

  def relationToList[A](r: Relation[A]): Seq[A] = r match {
    case j: Join[A]                => relationToList(j.left) ++ relationToList(j.right)
    case sr: SampledRelation[A, _] => relationToList(sr.relation)
    case ar: AliasedRelation[A, _] => relationToList(ar.relation)
    case t: Table[A]               => Seq(t.name)
  }

  implicit val catalog = new Catalog(
    HashMap(
      "db" -> HashMap(
        "foo" -> Seq("a", "b", "c"),
        "bar" -> Seq("x", "y", "z")
      )))

  case class ResolvedSelectItems(value: List[String])
  case class ResolvedRelations(value: List[String])
  case class Resolutions(rsi: ResolvedSelectItems, rr: ResolvedRelations)
  def resolve[R](q: QueryNoWith[R, String])(implicit schema: Catalog): Option[Resolutions] = {
    q.querySpecification.map { qs =>
      val ss: List[String] = qs.select.selectItems.flatMap {
        _ match {
          // TODO BAD CAST
          case s: SingleColumn[_] => {
            s.expression match { case i: Identifier[_] => Some(i.name.asInstanceOf[String]); case _ => None }
          }
          case a: AllColumns => a.name.map(_.name)
        }
      }
      val resolvedSelect = ResolvedSelectItems(ss.flatMap(schema.nameColumn))
      val resolvedRelations: ResolvedRelations = ResolvedRelations(qs.from.relations
        .map { relation =>
          // TODO specifying a function like rf should be considerably easier
          def rf(r: R): String = r match { case q: QualifiedName => q.name }
          mapRelation(rf)(relation)
        }
        .flatMap(relationToList)
        .flatMap(schema.nameTable))
      Resolutions(resolvedSelect, resolvedRelations)
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
