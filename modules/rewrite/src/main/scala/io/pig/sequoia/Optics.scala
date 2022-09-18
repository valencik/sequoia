/*
 * Copyright 2022 Pig.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pig.sequoia

import monocle.Lens
import monocle.Traversal
import monocle.Prism
import cats.implicits._
import cats.Applicative

object Optics {

  def tableNamesFromQuery[I]: Traversal[Query[I, RawName], RawTableName] =
    namesFromQuery.andThen(rawTableName)

  def columnNamesFromQuery[I]: Traversal[Query[I, RawName], RawColumnName] =
    namesFromQuery.andThen(rawColumnName)

  private def rawTableName[I]: Prism[RawName, RawTableName] =
    Prism.partial[RawName, RawTableName] { case r: RawTableName => r }(identity)

  private def rawColumnName[I]: Prism[RawName, RawColumnName] =
    Prism.partial[RawName, RawColumnName] { case r: RawColumnName => r }(identity)

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

  def namesFromQuery[I, R]: Traversal[Query[I, R], R] =
    new Traversal[Query[I, R], R] {
      def modifyA[F[_]: Applicative](f: R => F[R])(s: Query[I, R]): F[Query[I, R]] = {
        val cte: F[Option[With[I, R]]] = s.cte.traverse(namesFromWith.modifyA(f)(_))
        val qnw                        = namesFromQueryNoWith.modifyA(f)(s.queryNoWith)
        qnw.product(cte).map { case (q, c) => s.copy(cte = c, queryNoWith = q) }
      }
    }

  private def namesFromWith[I, R]: Traversal[With[I, R], R] =
    new Traversal[With[I, R], R] {
      def modifyA[F[_]: Applicative](f: R => F[R])(s: With[I, R]): F[With[I, R]] = {
        s.namedQueries
          .traverse(namesFromNamedQuery.modifyA(f))
          .map(nqs => s.copy(namedQueries = nqs))
      }
    }

  private def queryFromNamedQuery[I, R]: Lens[NamedQuery[I, R], Query[I, R]] =
    Lens[NamedQuery[I, R], Query[I, R]](_.query)(q => nq => nq.copy(query = q))

  private def namesFromNamedQuery[I, R]: Traversal[NamedQuery[I, R], R] =
    new Traversal[NamedQuery[I, R], R] {
      def modifyA[F[_]: Applicative](f: R => F[R])(s: NamedQuery[I, R]): F[NamedQuery[I, R]] = {
        val nfq: Traversal[Query[I, R], R] = namesFromQuery
        val lens: Traversal[NamedQuery[I, R], R] =
          queryFromNamedQuery.andThen(nfq)
        lens.modifyA(f)(s)
      }
    }

  private def queryTermFromQueryNoWith[I, R]: Lens[QueryNoWith[I, R], QueryTerm[I, R]] =
    Lens[QueryNoWith[I, R], QueryTerm[I, R]](_.queryTerm)(qt => qnw => qnw.copy(queryTerm = qt))

  private def namesFromQueryTerm[I, R]: Traversal[QueryTerm[I, R], R] =
    new Traversal[QueryTerm[I, R], R] {
      def modifyA[F[_]: Applicative](f: R => F[R])(s: QueryTerm[I, R]): F[QueryTerm[I, R]] = {
        s match {
          case qp: QueryPrimary[I, R] => namesFromQueryPrimary.modifyA(f)(qp).widen
          case so: SetOperation[I, R] => {
            val left  = modifyA(f)(so.left)
            val right = modifyA(f)(so.right)
            left.product(right).map { case (l, r) => so.copy(left = l, right = r) }
          }
        }
      }
    }

  private def namesFromQueryNoWith[I, R]: Traversal[QueryNoWith[I, R], R] = {
    val nfqt: Traversal[QueryTerm[I, R], R] = namesFromQueryTerm
    queryTermFromQueryNoWith.andThen(nfqt)
  }

  private def queryNoWithFromSubQuery[I, R]: Lens[SubQuery[I, R], QueryNoWith[I, R]] =
    Lens[SubQuery[I, R], QueryNoWith[I, R]](_.queryNoWith)(qnw => sq => sq.copy(queryNoWith = qnw))

  private def namesFromQueryPrimary[I, R]: Traversal[QueryPrimary[I, R], R] =
    new Traversal[QueryPrimary[I, R], R] {
      def modifyA[F[_]: Applicative](f: R => F[R])(s: QueryPrimary[I, R]): F[QueryPrimary[I, R]] = {
        s match {
          case qs: QuerySpecification[I, R] => {
            val select = qs.selectItems.traverse(namesFromSelectItem.modifyA(f)(_))
            val from   = qs.from.traverse(namesFromRelation.modifyA(f)(_))
            val where  = qs.where.traverse(namesFromExpression.modifyA(f)(_))
            val group  = qs.groupBy.traverse(namesFromGroupBy.modifyA(f)(_))
            val having = qs.having.traverse(namesFromExpression.modifyA(f)(_))
            (select, from, where, group, having).mapN { case (s, f, w, g, h) =>
              qs.copy(selectItems = s, from = f, where = w, groupBy = g, having = h)
            }
          }
          case qp: QueryPrimaryTable[I, R] =>
            f(qp.table.value).map(r => qp.copy(table = qp.table.copy(value = r)))
          case sq: SubQuery[I, R] => {
            val nfqnw: Traversal[QueryNoWith[I, R], R] = namesFromQueryNoWith
            val lens: Traversal[SubQuery[I, R], R] =
              queryNoWithFromSubQuery.andThen(nfqnw)
            lens.modifyA(f)(sq).widen
          }
          case _: InlineTable[I, R] => ???
        }
      }
    }

  private def namesFromSelectItem[I, R]: Traversal[SelectItem[I, R], R] =
    new Traversal[SelectItem[I, R], R] {
      def modifyA[F[_]: Applicative](f: R => F[R])(s: SelectItem[I, R]): F[SelectItem[I, R]] = {
        s match {
          case sa: SelectAll[I, R] =>
            sa.ref.traverse(nameFromTableRef.modifyA(f)(_)).map(r => sa.copy(ref = r))
          case ss: SelectSingle[I, R] =>
            namesFromExpression.modifyA(f)(ss.expr).map(e => ss.copy(expr = e))
        }
      }
    }

  private def namesFromGroupBy[I, R]: Traversal[GroupBy[I, R], R] =
    new Traversal[GroupBy[I, R], R] {
      def modifyA[F[_]: Applicative](f: R => F[R])(s: GroupBy[I, R]): F[GroupBy[I, R]] = {
        s.groupingElements
          .traverse(namesFromGroupingElement.modifyA(f)(_))
          .map(gs => s.copy(groupingElements = gs))
      }
    }

  def namesFromGroupingElement[I, R]: Traversal[GroupingElement[I, R], R] =
    new Traversal[GroupingElement[I, R], R] {
      def modifyA[F[_]: Applicative](
          f: R => F[R]
      )(s: GroupingElement[I, R]): F[GroupingElement[I, R]] = {
        s match {
          case s: SingleGroupingSet[I, R] =>
            namesFromGroupingSet.modifyA(f)(s.groupingSet).map(gs => s.copy(groupingSet = gs))
          case s: Rollup[I, R] =>
            s.expressions
              .traverse(namesFromExpression.modifyA(f)(_))
              .map(es => s.copy(expressions = es))
          case s: Cube[I, R] =>
            s.expressions
              .traverse(namesFromExpression.modifyA(f)(_))
              .map(es => s.copy(expressions = es))
          case s: MultipleGroupingSets[I, R] =>
            s.groupingSets
              .traverse(namesFromGroupingSet.modifyA(f)(_))
              .map(gs => s.copy(groupingSets = gs))
        }
      }
    }

  def namesFromGroupingSet[I, R]: Traversal[GroupingSet[I, R], R] =
    new Traversal[GroupingSet[I, R], R] {
      def modifyA[F[_]: Applicative](f: R => F[R])(s: GroupingSet[I, R]): F[GroupingSet[I, R]] = {
        s.expressions
          .traverse(namesFromExpression.modifyA(f)(_))
          .map(es => s.copy(expressions = es))
      }
    }

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

  private def namesFromRelation[I, R]: Traversal[Relation[I, R], R] =
    new Traversal[Relation[I, R], R] {
      def modifyA[F[_]: Applicative](f: R => F[R])(s: Relation[I, R]): F[Relation[I, R]] = {
        s match {
          case jr: JoinRelation[I, R] => {
            val fleft: F[Relation[I, R]]  = modifyA(f)(jr.left).widen
            val fright: F[Relation[I, R]] = modifyA(f)(jr.right).widen
            fleft.product(fright).map { case (l, r) => jr.copy(left = l, right = r) }
          }
          case sr: SampledRelation[I, R] => {
            val nfp: Traversal[RelationPrimary[I, R], R] = namesFromPrimary
            val lens: Traversal[SampledRelation[I, R], R] =
              primaryFromSampled.andThen(nfp)
            lens.modifyA(f)(sr).widen
          }
        }
      }
    }

  private def primaryFromSampled[I, R]: Lens[SampledRelation[I, R], RelationPrimary[I, R]] =
    Lens[SampledRelation[I, R], RelationPrimary[I, R]](_.aliasedRelation.relationPrimary) {
      rp => sr => sr.copy(aliasedRelation = sr.aliasedRelation.copy(relationPrimary = rp))
    }

  private def namesFromPrimary[I, R]: Traversal[RelationPrimary[I, R], R] =
    new Traversal[RelationPrimary[I, R], R] {
      def modifyA[F[_]: Applicative](
          f: R => F[R]
      )(s: RelationPrimary[I, R]): F[RelationPrimary[I, R]] = {
        s match {
          case tn: TableName[I, R] => nameFromTable.modifyA(f)(tn).widen
          case pr: ParenthesizedRelation[I, R] => {
            val nfr: Traversal[Relation[I, R], R] = namesFromRelation
            val lens: Traversal[ParenthesizedRelation[I, R], R] =
              relationFromParenthesized.andThen(nfr)
            lens.modifyA(f)(pr).widen
          }
          case _: SubQueryRelation[I, R] => ??? // TODO Requires Query support
          case _: Unnest[I, R]           => ??? // TODO Requires Expression support
          case _: LateralRelation[I, R]  => ??? // TODO Requires Query support
        }
      }
    }

  private def nameFromTableRef[I, R]: Lens[TableRef[I, R], R] =
    Lens[TableRef[I, R], R](_.value)(r => tf => tf.copy(value = r))

  private def nameFromTable[I, R]: Lens[TableName[I, R], R] =
    Lens[TableName[I, R], R](_.ref.value)(r => tn => tn.copy(ref = tn.ref.copy(value = r)))

  private def relationFromParenthesized[I, R]: Lens[ParenthesizedRelation[I, R], Relation[I, R]] =
    Lens[ParenthesizedRelation[I, R], Relation[I, R]](_.relation)(r => pr => pr.copy(relation = r))

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

  private def namesFromExpression[I, R]: Traversal[Expression[I, R], R] =
    new Traversal[Expression[I, R], R] {
      def modifyA[F[_]: Applicative](f: R => F[R])(s: Expression[I, R]): F[Expression[I, R]] = {
        s match {
          case lb: LogicalBinary[I, R] => {
            val left  = namesFromExpression.modifyA(f)(lb.left)
            val right = namesFromExpression.modifyA(f)(lb.right)
            left.product(right).map { case (l, r) => lb.copy(left = l, right = r) }
          }
          case p: Predicate[I, R]        => namesFromPredicate.modifyA(f)(p).widen
          case ve: ValueExpression[I, R] => namesFromValueExpression.modifyA(f)(ve).widen
        }
      }
    }

  private def namesFromPredicate[I, R]: Traversal[Predicate[I, R], R] =
    new Traversal[Predicate[I, R], R] {
      def modifyA[F[_]: Applicative](f: R => F[R])(s: Predicate[I, R]): F[Predicate[I, R]] = {
        s match {
          case pe: NotPredicate[I, R] =>
            namesFromExpression.modifyA(f)(pe.exp).map(e => pe.copy(exp = e))
          case pe: ComparisonExpr[I, R] => {
            val left  = namesFromValueExpression.modifyA(f)(pe.left)
            val right = namesFromValueExpression.modifyA(f)(pe.right)
            left.product(right).map { case (l, r) => pe.copy(left = l, right = r) }
          }
          case pe: QuantifiedComparison[I, R] => {
            val value = namesFromValueExpression.modifyA(f)(pe.value)
            val query = namesFromQuery.modifyA(f)(pe.query)
            value.product(query).map { case (v, q) => pe.copy(value = v, query = q) }
          }
          case pe: Between[I, R] => {
            val lower = namesFromValueExpression.modifyA(f)(pe.lower)
            val upper = namesFromValueExpression.modifyA(f)(pe.upper)
            val value = namesFromValueExpression.modifyA(f)(pe.value)
            lower.product(upper).product(value).map { case ((l, u), v) =>
              pe.copy(lower = l, upper = u, value = v)
            }
          }
          case pe: InList[I, R] => {
            val value = namesFromValueExpression.modifyA(f)(pe.value)
            val exps  = pe.exps.traverse(namesFromExpression.modifyA(f)(_))
            value.product(exps).map { case (v, es) => pe.copy(value = v, exps = es) }
          }
          case pe: InSubQuery[I, R] => {
            val value = namesFromValueExpression.modifyA(f)(pe.value)
            val query = namesFromQuery.modifyA(f)(pe.query)
            value.product(query).map { case (v, q) => pe.copy(value = v, query = q) }
          }
          case pe: Like[I, R] => {
            val escape  = pe.escape.traverse(namesFromValueExpression.modifyA(f)(_))
            val pattern = namesFromValueExpression.modifyA(f)(pe.pattern)
            val value   = namesFromValueExpression.modifyA(f)(pe.value)
            escape.product(pattern).product(value).map { case ((e, p), v) =>
              pe.copy(escape = e, pattern = p, value = v)
            }
          }
          case pe: NullPredicate[I, R] =>
            namesFromValueExpression.modifyA(f)(pe.value).map(e => pe.copy(value = e))
          case pe: DistinctFrom[I, R] => {
            val value = namesFromValueExpression.modifyA(f)(pe.value)
            val right = namesFromValueExpression.modifyA(f)(pe.right)
            value.product(right).map { case (v, r) => pe.copy(value = v, right = r) }
          }
        }
      }
    }

  private def namesFromValueExpression[I, R]: Traversal[ValueExpression[I, R], R] =
    new Traversal[ValueExpression[I, R], R] {
      def modifyA[F[_]: Applicative](
          f: R => F[R]
      )(s: ValueExpression[I, R]): F[ValueExpression[I, R]] = {
        s match {
          case au: ArithmeticUnary[I, R] =>
            namesFromValueExpression.modifyA(f)(au).map(v => au.copy(value = v))
          case ab: ArithmeticBinary[I, R] => {
            val left  = namesFromValueExpression.modifyA(f)(ab.left)
            val right = namesFromValueExpression.modifyA(f)(ab.right)
            left.product(right).map { case (l, r) => ab.copy(left = l, right = r) }
          }
          case pe: PrimaryExpression[I, R] => namesFromPrimaryExpression.modifyA(f)(pe).widen
        }
      }
    }

  private def namesFromPrimaryExpression[I, R]: Traversal[PrimaryExpression[I, R], R] =
    new Traversal[PrimaryExpression[I, R], R] {
      def modifyA[F[_]: Applicative](
          f: R => F[R]
      )(s: PrimaryExpression[I, R]): F[PrimaryExpression[I, R]] = {
        s match {
          // TODO How am I going to get the String from StringLiteral out here?
          // I don't think I can unless I come up with more 'Raw' types
          // Or we change every optic written so far to return a different type instead of R...
          case pe: LiteralExpr[I, R] => pe.pure[F].widen
          case pe: ColumnExpr[I, R]  => nameFromColumnExpr.modifyA(f)(pe).widen
          case pe: SubQueryExpr[I, R] =>
            namesFromQuery.modifyA(f)(pe.query).map(q => pe.copy(query = q))
          case pe: ExistsExpr[I, R] =>
            namesFromQuery.modifyA(f)(pe.query).map(q => pe.copy(query = q))
          case pe: SimpleCase[I, R] => {
            val exp  = namesFromExpression.modifyA(f)(pe.exp)
            val when = pe.whenClauses.traverse(namesFromWhenClause.modifyA(f)(_))
            val el   = pe.elseExpression.traverse(namesFromExpression.modifyA(f)(_))
            exp.product(when).product(el).map { case ((e, w), l) =>
              pe.copy(exp = e, whenClauses = w, elseExpression = l)
            }
          }
          case pe: SearchedCase[I, R] => {
            val when = pe.whenClauses.traverse(namesFromWhenClause.modifyA(f)(_))
            val el   = pe.elseExpression.traverse(namesFromExpression.modifyA(f)(_))
            when.product(el).map { case (w, l) =>
              pe.copy(whenClauses = w, elseExpression = l)
            }

          }
          case pe: Cast[I, R] =>
            namesFromExpression.modifyA(f)(pe.exp).map(e => pe.copy(exp = e))
          case pe: Subscript[I, R] => {
            val value = namesFromPrimaryExpression.modifyA(f)(pe.value)
            val index = namesFromValueExpression.modifyA(f)(pe.index)
            value.product(index).map { case (v, i) => pe.copy(value = v, index = i) }
          }
          case pe: DereferenceExpr[I, R] =>
            namesFromPrimaryExpression.modifyA(f)(pe.base).map(e => pe.copy(base = e))
          case pe: Row[I, R] => {
            val exps = pe.exps.traverse(namesFromExpression.modifyA(f)(_))
            exps.map(es => pe.copy(exps = es))
          }
          case pe: FunctionCall[I, R] => {
            val exps = pe.exprs.traverse(namesFromExpression.modifyA(f)(_))
            exps.map(es => pe.copy(exprs = es))
          }
          case pe: IntervalLiteral[I, R] =>
            pe.pure[F].widen // TODO there is a string literal in here
          case pe: SpecialDateTimeFunc[I, R] => pe.pure[F].widen
          case pe: Extract[I, R] =>
            namesFromValueExpression.modifyA(f)(pe.exp).map(e => pe.copy(exp = e))
        }
      }
    }

  private def nameFromColumnExpr[I, R]: Lens[ColumnExpr[I, R], R] =
    Lens[ColumnExpr[I, R], R](_.col.value)(v => ce => ce.copy(col = ce.col.copy(value = v)))

  private def namesFromWhenClause[I, R]: Traversal[WhenClause[I, R], R] =
    new Traversal[WhenClause[I, R], R] {
      def modifyA[F[_]: Applicative](f: R => F[R])(s: WhenClause[I, R]): F[WhenClause[I, R]] = {
        val cond   = namesFromExpression.modifyA(f)(s.condition)
        val result = namesFromExpression.modifyA(f)(s.result)
        cond.product(result).map { case (c, r) => s.copy(condition = c, result = r) }
      }
    }

}
