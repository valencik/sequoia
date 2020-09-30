package ca.valencik.sequoia

import monocle.Lens
import monocle.Traversal
import monocle.Prism
import cats.implicits._
import cats.Applicative

object Lenses {

  // Relation
  // -- JoinRelation: Relation, Relation
  // -- SampledRelation: AliasedRelation
  //
  // AliasedRelation: RelationPrimary
  //
  // RelationPrimary
  // -- TableName: TableRef
  // -- SubQueryRelation: Query
  // -- Unnest: List[Expression]
  // -- LateralRelation: Query
  // -- ParenthesizedRelation: Relation
  //
  // TableRef: R

  private def rawTableName[I]: Prism[RawName, RawTableName] =
    Prism.partial[RawName, RawTableName] { case r: RawTableName => r }(identity)

  private def rawColumnName[I]: Prism[RawName, RawColumnName] =
    Prism.partial[RawName, RawColumnName] { case r: RawColumnName => r }(identity)

  def tableNamesFromQuery[I]: Traversal[Query[I, RawName], RawTableName] =
    relationsFromQuery.composePrism(rawTableName)

  def columnNamesFromQuery[I]: Traversal[Query[I, RawName], RawColumnName] =
    relationsFromQuery.composePrism(rawColumnName)

  private def nameFromTable[I, R]: Lens[TableName[I, R], R] =
    Lens[TableName[I, R], R](_.ref.value)(r => tn => tn.copy(ref = tn.ref.copy(value = r)))

  private def relationFromParenthesized[I, R]: Lens[ParenthesizedRelation[I, R], Relation[I, R]] =
    Lens[ParenthesizedRelation[I, R], Relation[I, R]](_.relation)(r => pr => pr.copy(relation = r))

  private def primaryFromSampled[I, R]: Lens[SampledRelation[I, R], RelationPrimary[I, R]] =
    Lens[SampledRelation[I, R], RelationPrimary[I, R]](_.aliasedRelation.relationPrimary) {
      rp => sr => sr.copy(aliasedRelation = sr.aliasedRelation.copy(relationPrimary = rp))
    }

  private def namesFromPrimary[I, R]: Traversal[RelationPrimary[I, R], R] =
    new Traversal[RelationPrimary[I, R], R] {
      def modifyF[F[_]: Applicative](
          f: R => F[R]
      )(s: RelationPrimary[I, R]): F[RelationPrimary[I, R]] = {
        s match {
          case tn: TableName[I, R] => nameFromTable.modifyF(f)(tn).widen
          case pr: ParenthesizedRelation[I, R] => {
            val lens: Traversal[ParenthesizedRelation[I, R], R] =
              relationFromParenthesized.composeTraversal(relationNames)
            lens.modifyF(f)(pr).widen
          }
          case _: SubQueryRelation[I, R] => ??? //TODO Requires Query support
          case _: Unnest[I, R]           => ??? //TODO Requires Expression support
          case _: LateralRelation[I, R]  => ??? //TODO Requires Query support
        }
      }
    }

  def relationNames[I, R]: Traversal[Relation[I, R], R] =
    new Traversal[Relation[I, R], R] {
      def modifyF[F[_]: Applicative](f: R => F[R])(s: Relation[I, R]): F[Relation[I, R]] = {
        s match {
          case jr: JoinRelation[I, R] => {
            val fleft: F[Relation[I, R]]  = modifyF(f)(jr.left).widen
            val fright: F[Relation[I, R]] = modifyF(f)(jr.right).widen
            fleft.product(fright).map { case (l, r) => jr.copy(left = l, right = r) }
          }
          case sr: SampledRelation[I, R] => {
            val lens: Traversal[SampledRelation[I, R], R] =
              primaryFromSampled.composeTraversal(namesFromPrimary)
            lens.modifyF(f)(sr).widen
          }
        }
      }
    }

  // Query: Option[With], QueryNoWith
  //
  // With: List[NamedQuery]
  //
  // NamedQuery: Query
  //
  // QueryNoWith: QueryTerm
  //
  // QueryTerm
  // -- QueryPrimary
  // -- SetOperation: QueryTerm, QueryTerm
  //
  // QueryPrimary
  // -- QuerySpecification: List[Relation], Option[Expression], Option[Expression]
  // -- QueryPrimaryTable: TableRef
  // -- InlineTable: List[Expression]
  // -- SubQuery: QueryNoWith

  def relationsFromQuery[I, R]: Traversal[Query[I, R], R] =
    new Traversal[Query[I, R], R] {
      def modifyF[F[_]: Applicative](f: R => F[R])(s: Query[I, R]): F[Query[I, R]] = {
        val cte: F[Option[With[I, R]]] = s.cte.traverse(relationsFromWith.modifyF(f)(_))
        val qnw                        = relationsFromQueryNoWith.modifyF(f)(s.queryNoWith)
        qnw.product(cte).map { case (q, c) => s.copy(cte = c, queryNoWith = q) }
      }
    }

  private def relationsFromWith[I, R]: Traversal[With[I, R], R] =
    new Traversal[With[I, R], R] {
      def modifyF[F[_]: Applicative](f: R => F[R])(s: With[I, R]): F[With[I, R]] = {
        s.namedQueries
          .traverse(relationsFromNamedQuery.modifyF(f))
          .map(nqs => s.copy(namedQueries = nqs))
      }
    }

  private def queryFromNamedQuery[I, R]: Lens[NamedQuery[I, R], Query[I, R]] =
    Lens[NamedQuery[I, R], Query[I, R]](_.query)(q => nq => nq.copy(query = q))

  private def relationsFromNamedQuery[I, R]: Traversal[NamedQuery[I, R], R] =
    new Traversal[NamedQuery[I, R], R] {
      def modifyF[F[_]: Applicative](f: R => F[R])(s: NamedQuery[I, R]): F[NamedQuery[I, R]] = {
        val lens: Traversal[NamedQuery[I, R], R] =
          queryFromNamedQuery.composeTraversal(relationsFromQuery)
        lens.modifyF(f)(s)
      }
    }

  private def queryTermFromQueryNoWith[I, R]: Lens[QueryNoWith[I, R], QueryTerm[I, R]] =
    Lens[QueryNoWith[I, R], QueryTerm[I, R]](_.queryTerm)(qt => qnw => qnw.copy(queryTerm = qt))

  private def relationsFromQueryTerm[I, R]: Traversal[QueryTerm[I, R], R] =
    new Traversal[QueryTerm[I, R], R] {
      def modifyF[F[_]: Applicative](f: R => F[R])(s: QueryTerm[I, R]): F[QueryTerm[I, R]] = {
        s match {
          case qp: QueryPrimary[I, R] => relationsFromQueryPrimary.modifyF(f)(qp).widen
          case so: SetOperation[I, R] => {
            val left  = modifyF(f)(so.left)
            val right = modifyF(f)(so.right)
            left.product(right).map { case (l, r) => so.copy(left = l, right = r) }
          }
        }
      }
    }

  private def relationsFromQueryNoWith[I, R]: Traversal[QueryNoWith[I, R], R] =
    queryTermFromQueryNoWith.composeTraversal(relationsFromQueryTerm)

  private def queryNoWithFromSubQuery[I, R]: Lens[SubQuery[I, R], QueryNoWith[I, R]] =
    Lens[SubQuery[I, R], QueryNoWith[I, R]](_.queryNoWith)(qnw => sq => sq.copy(queryNoWith = qnw))

  private def relationsFromQueryPrimary[I, R]: Traversal[QueryPrimary[I, R], R] =
    new Traversal[QueryPrimary[I, R], R] {
      def modifyF[F[_]: Applicative](f: R => F[R])(s: QueryPrimary[I, R]): F[QueryPrimary[I, R]] = {
        s match {
          case qs: QuerySpecification[I, R] => {
            val from  = qs.from.traverse(relationNames.modifyF(f)(_))
            val where = qs.where.traverse(namesFromExpression.modifyF(f)(_))
            from.product(where).map { case (f, w) => qs.copy(from = f, where = w) }
          }
          case qp: QueryPrimaryTable[I, R] =>
            f(qp.table.value).map(r => qp.copy(table = qp.table.copy(value = r)))
          case sq: SubQuery[I, R] => {
            val lens: Traversal[SubQuery[I, R], R] =
              queryNoWithFromSubQuery.composeTraversal(relationsFromQueryNoWith)
            lens.modifyF(f)(sq).widen
          }
          case _: InlineTable[I, R] => ???
        }
      }
    }

  // Expression
  // -- LogicalBinary: Expression, Expression
  // -- Predicate
  // -- ValueExpession
  //
  // Predicate
  // -- NotPredicate: Expression
  // -- ComparsionExpr: ValueExpression, ValueExpression
  // -- QuantifiedComparison: ValueExpression, Query
  // -- Between: ValueExpression, ValueExpression, ValueExpression
  // -- InList: ValueExpression, List[Expression]
  // -- InSubQuery: ValueExpression, Query
  // -- Like: ValueExpression, ValueExpression, Option[ValueExpression]
  // -- NullPredicate: ValueExpression
  // -- DistinctFrom: ValueExpression, ValueExpression
  //
  // ValueExpression
  // -- ArithmeticUnary: ValueExpression
  // -- ArithmeticBinary: ValueExpression, ValueExpression
  // -- PrimaryExpression
  //
  // PrimaryExpression
  // -- LiteralExpr
  // -- ColumnExpr: ColumnRef
  // -- SubQueryExpr: Query
  // -- ExistsExpr: Query
  // -- SimpleCase: ValueExpression, Option[Expression]
  // -- SearchedCase: Option[Expression]
  // -- Cast: Expression
  // -- Subscript: PrimaryExpression, ValueExpression
  // -- DereferenceExpr: PrimaryExpression
  // -- Row: List[Expression]
  // -- FunctionCall: List[Exprssion]
  // -- IntervalLiteral
  // -- SpecialDateTimeFunc
  // -- Extract: ValueExpression

  private def nameFromColumnExpr[I, R]: Lens[ColumnExpr[I, R], R] =
    Lens[ColumnExpr[I, R], R](_.col.value)(v => ce => ce.copy(col = ce.col.copy(value = v)))

  def namesFromExpression[I, R]: Traversal[Expression[I, R], R] =
    new Traversal[Expression[I, R], R] {
      def modifyF[F[_]: Applicative](f: R => F[R])(s: Expression[I, R]): F[Expression[I, R]] = {
        s match {
          case ve: ValueExpression[I, R] => namesFromValueExpression.modifyF(f)(ve).widen
          case p: Predicate[I, R]        => namesFromPredicate.modifyF(f)(p).widen
          case x                         => x.pure[F]
        }
      }
    }

  def namesFromPredicate[I, R]: Traversal[Predicate[I, R], R] =
    new Traversal[Predicate[I, R], R] {
      def modifyF[F[_]: Applicative](f: R => F[R])(s: Predicate[I, R]): F[Predicate[I, R]] = {
        s match {
          case ce: ComparisonExpr[I, R] => {
            val left  = namesFromValueExpression.modifyF(f)(ce.left)
            val right = namesFromValueExpression.modifyF(f)(ce.right)
            left.product(right).map { case (l, r) => ce.copy(left = l, right = r) }
          }
          case x => x.pure[F]
        }
      }
    }

  private def namesFromValueExpression[I, R]: Traversal[ValueExpression[I, R], R] =
    new Traversal[ValueExpression[I, R], R] {
      def modifyF[F[_]: Applicative](
          f: R => F[R]
      )(s: ValueExpression[I, R]): F[ValueExpression[I, R]] = {
        s match {
          case pe: PrimaryExpression[I, R] => namesFromPrimaryExpression.modifyF(f)(pe).widen
          case x                           => x.pure[F]
        }
      }
    }

  private def namesFromPrimaryExpression[I, R]: Traversal[PrimaryExpression[I, R], R] =
    new Traversal[PrimaryExpression[I, R], R] {
      def modifyF[F[_]: Applicative](
          f: R => F[R]
      )(s: PrimaryExpression[I, R]): F[PrimaryExpression[I, R]] = {
        s match {
          case ce: ColumnExpr[I, R] => nameFromColumnExpr.modifyF(f)(ce).widen
          case x                    => x.pure[F]
        }
      }
    }

}

object LensApp {

  import Lenses._
  def simpleRelation(name: String) =
    SampledRelation(1, AliasedRelation(2, TableName(3, TableRef(4, name)), None, None), None)

  def joinRelation(left: String, right: String) =
    JoinRelation(1, InnerJoin, simpleRelation(left), simpleRelation(right), None)

  def main(args: Array[String]): Unit = {

    val foobar = joinRelation("foo", "bar")

    val upperIfFoo = relationNames[Int, String].modify {
      case r => if (r.startsWith("foo")) r.toUpperCase else r
    }
    println("Uppercasing relations starting with 'foo':")
    println(upperIfFoo(foobar))

  }
}
