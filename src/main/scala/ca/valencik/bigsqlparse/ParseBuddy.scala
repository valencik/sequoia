package ca.valencik.bigsqlparse

import scala.collection.mutable.HashMap
import org.antlr.v4.runtime.{BaseErrorListener, CharStreams, CommonTokenStream, RecognitionException, Recognizer}

sealed trait ResolvableRelation
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

  def parse(input: String): Either[ParseFailure, Query[QualifiedName, RawIdentifier]] = {
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
      val qnw        = node.asInstanceOf[Query[QualifiedName, RawIdentifier]]
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

  val catalog = Catalog(
    HashMap(
      "db" -> HashMap(
        "foo" -> Seq("a", "b", "c"),
        "bar" -> Seq("x", "y", "z")
      )))

  def resolveRelation(acc: Catalog, alias: Option[RawIdentifier], qn: QualifiedName): ResolvableRelation = {
    acc
      .lookupAndMaybeModify(alias, qn)
      .map { q =>
        ResolvedRelation(q.name)
      }
      .getOrElse(UnresolvedRelation(qn.name))
  }
  def resolveRelations(acc: Catalog,
                       q: Query[QualifiedName, RawIdentifier],
                       alias: Option[RawIdentifier]): Query[ResolvableRelation, RawIdentifier] = {
    val queriesResolved = q.withz.map { w =>
      w.queries.map { wqs =>
        // each one of these withquery names needs to become a temp view
        wqs.copy(query = resolveRelations(acc, wqs.query, Some(wqs.name.name)))
      }
    }
    // the resolution here needs to use the updated catalog with temp views
    val qnwr = q.queryNoWith.querySpecification.from.relations.map { rs =>
      val resolvedRelations: List[Relation[ResolvableRelation]] = rs.map(mapRelation { qn: QualifiedName =>
        resolveRelation(acc, alias, qn)
      }(_))
      println(s"-- QNW Resolution with alias: ${alias} and resolvedRelations: ${resolvedRelations}")
      resolvedRelations
    }

    val withR = q.withz.flatMap { w: With[QualifiedName, RawIdentifier] =>
      {
        queriesResolved.map { qrs =>
          w.copy(queries = qrs)
        }
      }
    }
    val from: From[ResolvableRelation]                         = q.queryNoWith.querySpecification.from.copy(relations = qnwr)
    val queryS: QuerySpecification[ResolvableRelation, RawIdentifier] = q.queryNoWith.querySpecification.copy(from = from)
    val qnw: QueryNoWith[ResolvableRelation, RawIdentifier]           = q.queryNoWith.copy(querySpecification = queryS)
    Query(withR, qnw)
  }

  // TODO Handle ambiguity
  def resolveColumn(acc: Catalog, c: Identifier[_], relations: List[ResolvableRelation]): Option[String] = {
    println(acc)
    relations.flatMap {r => acc.lookupColumnInRelation(c, r)}.headOption
  }

  def resolveReferences[R](acc: Catalog, q: Query[ResolvableRelation, RawIdentifier]): Query[ResolvableRelation, RawIdentifier] = {
    val qs = q.queryNoWith.querySpecification
    val resolvedRelations: List[ResolvableRelation] = qs.from.relations
      .map { r =>
        r.flatMap(relationToList(_))
      }
      .getOrElse(List.empty)
    println(s"(resolveReferences) Resolved Relations: $resolvedRelations")

    val selectItemsResolved = qs.select.selectItems.map { si =>
      val sim: Option[String] = si match {
        case sc: SingleColumn[_] =>
          // TODO perhaps want expressionMap back here
          sc.expression match {
            case e: Identifier[_] => resolveColumn(acc, e, resolvedRelations)
            case _ => None
          }
        case _ => ???
      }
      println(s"(resolveReferences) si: $si, sim: $sim")
      val ref = sim match {
        case Some(rn) => SingleColumn(Identifier(ResolvedReference(rn)), None)
        case None     => SingleColumn(Identifier(UnresolvedReference("WTF?!")), None)
      }
      ref
    }
    val select = q.queryNoWith.querySpecification.select.copy(selectItems = selectItemsResolved)
    val queryS = q.queryNoWith.querySpecification.copy(select = select)
    val qnw    = q.queryNoWith.copy(querySpecification = queryS)
    q.copy(queryNoWith = qnw)
  }

}

object ParseBuddyApp extends App {
  import ca.valencik.bigsqlparse.ParseBuddy._

  private val exitCommands                  = Seq("exit", ":q", "q")
  def exitCommand(command: String): Boolean = exitCommands.contains(command.toLowerCase)

  def inputLoop(): Unit = {
    val inputQuery = scala.io.StdIn.readLine("\nParseBuddy> ")
    if (!exitCommand(inputQuery)) {
      val q = parse(inputQuery)
      println(s"\n(main) Parse: $q \n")
      q.right.map(qnw => {
        val resolved = resolveRelations(catalog, qnw, None)
        println(s"(main) Resolved Relations: ${resolved}")
        println(s"(main) Resolved Relations: ${resolved.show}")
      })
      inputLoop()
    }
  }

  inputLoop()
}
