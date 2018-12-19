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

  def resolveRelation(acc: Catalog, alias: Option[Identifier[String]], qn: QualifiedName): ResolvableRelation = {
    acc
      .lookupAndMaybeModify(alias, qn)
      .map { q =>
        ResolvedRelation(q.name)
      }
      .getOrElse(UnresolvedRelation(qn.name))
  }
  def resolveRelations(acc: Catalog,
                       q: Query[QualifiedName, String],
                       alias: Option[Identifier[String]]): Query[ResolvableRelation, String] = {
    val queriesResolved = q.withz.map { w =>
      w.queries.map { wqs =>
        // each one of these withquery names needs to become a temp view
        wqs.copy(query = resolveRelations(acc, wqs.query, Some(wqs.name)))
      }
    }
    // the resolution here needs to use the updated catalog with temp views
    val qnwr = q.queryNoWith.querySpecification.from.relations.map { rs =>
      val resolvedRelations: List[Relation[ResolvableRelation]] = rs.map(r =>
        r.map { qn: QualifiedName =>
          resolveRelation(acc, alias, qn)
      })
      resolvedRelations
    }

    val withR = q.withz.flatMap { w: With[QualifiedName, String] =>
      {
        queriesResolved.map { qrs =>
          w.copy(queries = qrs)
        }
      }
    }
    val from: From[ResolvableRelation]                         = q.queryNoWith.querySpecification.from.copy(relations = qnwr)
    val queryS: QuerySpecification[ResolvableRelation, String] = q.queryNoWith.querySpecification.copy(from = from)
    val qnw: QueryNoWith[ResolvableRelation, String]           = q.queryNoWith.copy(querySpecification = queryS)
    Query(withR, qnw)
  }

  def resolveColumn(acc: Catalog, c: Identifier[_], relations: List[ResolvableRelation]): ResolvableReference = {
    val col = c.name.asInstanceOf[String]
    println(s"Attemping to resolve column ${c.name}")
    val rcol = acc.lookupColumnStringInRelations(col, relations)
    rcol match {
      case Some(rc) => ResolvedReference(rc)
      case None     => UnresolvedReference(col)
    }
  }

  def resolveExpression(acc: Catalog,
                        e: Expression[_],
                        relations: List[ResolvableRelation]): Option[ResolvableReference] = e match {
    case e: Identifier[_] => Some(resolveColumn(acc, e, relations))
    case _                => None
  }

  def resolveReferences[R](acc: Catalog, q: Query[ResolvableRelation, String]): Query[ResolvableRelation, String] = {
    val qs = q.queryNoWith.querySpecification
    val resolvedRelations: List[ResolvableRelation] = qs.from.relations
      .map { r =>
        r.flatMap(_.toList)
      }
      .getOrElse(List.empty)
    println(s"(resolveReferences) Resolved Relations: $resolvedRelations")
    def innerResolveCol(c: String): Option[String] = {
      acc.lookupColumnStringInRelations(c, resolvedRelations)
    }

    val selectItemsResolved = qs.select.selectItems.map { si =>
      val sim: SelectItem = si match {
        case sc: SingleColumn[_] =>
          // TODO perhaps want expressionMap back here
          sc.expression match {
            case e: Identifier[_] => SingleColumn(Identifier(resolveColumn(acc, e, resolvedRelations)), None)
            case _                => ???
          }
        case _ => ???
      }
      sim
    }

    val fromR: From[ResolvableRelation] = From(qs.from.relations.map { rs =>
      rs.map(_.mapJoinExpression(_.asInstanceOf[Expression[String]].map(innerResolveCol)))
    })
    val whereR: Where[String] = Where(qs.where.expression.map(_.map(innerResolveCol(_).get)))
    val groupbyR: GroupBy[String] = GroupBy(qs.groupBy.groupingElements.map { ge =>
      val inner = ge.groupingSet.map { _.map(innerResolveCol(_).get) }
      GroupingElement(inner)
    })
    val select: Select = q.queryNoWith.querySpecification.select.copy(selectItems = selectItemsResolved)
    println(select)
    println(whereR)
    println(fromR)
    println(groupbyR)
    val queryS: QuerySpecification[ResolvableRelation, String] =
      qs.copy(select = select, where = whereR, groupBy = groupbyR, from = fromR)
    val qnw = q.queryNoWith.copy(querySpecification = queryS)
    q.copy(queryNoWith = qnw)
  }

  def getExpressionName[E](e: Expression[String]): Seq[String] = e match {
    case i: Identifier[String]            => Seq(i.name)
    case be: BooleanExpression[String]    => getExpressionName(be.left) ++ getExpressionName(be.right)
    case ce: ComparisonExpression[String] => getExpressionName(ce.left) ++ getExpressionName(ce.right)
    case inp: IsNullPredicate[String]     => getExpressionName(inp.value)
    case inn: IsNotNullPredicate[String]  => getExpressionName(inn.value)
  }

  case class AggregatedClauses(selects: Seq[String],
                               joins: Seq[String],
                               wheres: Seq[String],
                               groupbys: Seq[String],
                               havings: Seq[String],
                               orderbys: Seq[String]) {
    override def toString: String =
      s"""|SELECT: ${selects.mkString(", ")}
          |JOIN: ${joins.mkString(", ")}
          |WHERE: ${wheres.mkString(", ")}
          |GROUPBY: ${groupbys.mkString(", ")}
          |HAVING: ${havings.mkString(", ")}
          |ORDERBY: ${orderbys.mkString(", ")}
          |""".stripMargin
  }
  def extractReferencesByClause(q: Query[ResolvableRelation, String]): AggregatedClauses = {
    val qs = q.queryNoWith.querySpecification
    val selects: List[String] = qs.select.selectItems
      .map {
        case sc: SingleColumn[_] =>
          sc.expression match {
            case e: Identifier[_] => Option(e.name.asInstanceOf[ResolvedReference].value)
            case _                => None
          }
        case _ => None
      }
      .filter(_.isDefined)
      .map(_.get)
    val joins = qs.from.relations
      .map { rs =>
        rs.flatMap {
          case j: Join[_] =>
            j.criterea.map {
              case jo: JoinOn[_] => getExpressionName(jo.expression.asInstanceOf[Expression[String]])
              case _             => Seq.empty
            }
          case _ => Seq.empty
        }.flatten
      }
      .getOrElse(List.empty)
    val wheres = qs.where.expression.map(getExpressionName(_)).getOrElse(List.empty)
    val groupbys = qs.groupBy.groupingElements.flatMap { ge =>
      ge.groupingSet.flatMap { getExpressionName(_) }
    }
    val havings = qs.having.expression.map(getExpressionName(_)).getOrElse(List.empty)
    val orderbys = q.queryNoWith.orderBy
      .map { ob =>
        ob.items.flatMap { sortItem =>
          getExpressionName(sortItem.expression)
        }
      }
      .getOrElse(List.empty)

    AggregatedClauses(selects, joins, wheres, groupbys, havings, orderbys)
  }

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
      q.right.map(qnw => {
        val resolved = resolveRelations(catalog, qnw, None)
        println(s"(main) Resolved Relations: ${resolved}")
        val resolvedColumns = resolveReferences(catalog, resolved)
        println(s"(main) Resolved Columns: ${resolvedColumns}")

        println("Now for some clauses!")
        println(extractReferencesByClause(resolvedColumns))
      })
      inputLoop()
    }
  }

  inputLoop()
}
