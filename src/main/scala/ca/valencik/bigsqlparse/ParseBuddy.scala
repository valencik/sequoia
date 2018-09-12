package ca.valencik.bigsqlparse

import scala.collection.immutable.HashMap
import org.antlr.v4.runtime.{BaseErrorListener, CharStreams, CommonTokenStream, RecognitionException, Recognizer}

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

  def parse(input: String): Either[ParseFailure, QueryNoWith[QualifiedName, String]] = {
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
      val qnw        = node.asInstanceOf[QueryNoWith[QualifiedName, String]]
      if (qnw == null) Left(ParseFailure("oops")) else Right(qnw)
    } catch {
      case e: AntlrParseException => Left(ParseFailure(e.msg))
    }
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

  sealed trait ResolvableRelation
  case class ResolvedRelation(value: String)   extends ResolvableRelation
  case class UnresolvedRelation(value: String) extends ResolvableRelation
  def resolveRelations[R](q: QueryNoWith[R, String])(
      implicit catalog: Catalog): QueryNoWith[ResolvableRelation, String] = {
    val resolved = q.querySpecification.flatMap { qs =>
      qs.from.relations.map { rs =>
        rs.map { relation =>
          def rf(r: R): ResolvableRelation = r match {
            case q: QualifiedName => {
              catalog.nameTable(q.name) match {
                case Some(t) => ResolvedRelation(t)
                case None    => UnresolvedRelation(q.name)
              }

            }
          }
          mapRelation(rf)(relation)
        }
      }
    }
    val from   = q.querySpecification.get.from.copy(relations = resolved)
    val queryS = q.querySpecification.get.copy(from = from)
    val qnw    = q.copy(querySpecification = Some(queryS))
    qnw
  }

  sealed trait ResolvableReference
  case class ResolvedReference(value: String)   extends ResolvableReference
  case class UnresolvedReference(value: String) extends ResolvableReference

  // TODO Handle ambiguity
  def resolveColumn(c: String, relations: List[ResolvableRelation])(implicit catalog: Catalog): Option[String] =
    relations.flatMap { r =>
      val resoltion = r match {
        case r: ResolvedRelation => catalog.nameColumnInTable(r.value)(c)
        case _                   => None
      }
      println(s"Attempted to resolve $c with $r, and with result: $resoltion")
      resoltion
    }.headOption

  def resolveReferences[R](q: QueryNoWith[ResolvableRelation, String])(
      implicit catalog: Catalog): QueryNoWith[ResolvableRelation, String] = {
    val x = q.querySpecification.map { qs =>
      val resolvedRelations: List[ResolvableRelation] = qs.from.relations
        .map { r =>
          r.flatMap(relationToList(_))
        }
        .getOrElse(List.empty)
      qs.select.selectItems.map { si =>
        val sim: Option[String] = si match {
          case sc: SingleColumn[_] =>
            sc.expression match {
              case e: Identifier[_] => {
                val col = Option(e.name.asInstanceOf[String])
                col.flatMap { c =>
                  resolveColumn(c, resolvedRelations)
                }
              }
              case _ => None
            }
          case a: AllColumns =>
            a.name.flatMap { qn =>
              // TODO qualifiedname's actually have parts i need to handle
              catalog.nameTable(qn.name)
            }
        }
        println(s"si: $si, sim: $sim")
        val ref = sim match {
          case Some(rn) => SingleColumn(Identifier(ResolvedReference(rn)), None)
          case None     => SingleColumn(Identifier(UnresolvedReference("WTF?!")), None)
        }
        ref
      }
    }
    val select = q.querySpecification.get.select.copy(selectItems = x.get)
    val queryS = q.querySpecification.get.copy(select = select)
    val qnw    = q.copy(querySpecification = Some(queryS))
    qnw
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
      q.right.map(qnw => {
        val resolvedColumns = resolveReferences(resolveRelations(qnw))
        println(s"Resolved Columns: ${resolvedColumns}")
      })
      inputLoop()
    }
  }

  inputLoop()
}
