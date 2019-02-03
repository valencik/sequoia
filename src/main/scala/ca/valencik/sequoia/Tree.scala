package ca.valencik.sequoia

import scala.language.higherKinds
import cats.{Applicative, Eq, Eval, Functor, Traverse}
import cats.data.NonEmptyList
import cats.implicits._

sealed trait RawName {
  def value: String
}
final case class RawTableName(value: String)  extends RawName
final case class RawColumnName(value: String) extends RawName

sealed trait ResolvedName {
  def value: String
}
final case class ResolvedTableName(value: String)    extends ResolvedName
final case class ResolvedTableAlias(value: String)   extends ResolvedName
final case class ResolvedColumnName(value: String)   extends ResolvedName
final case class UnresolvedTableName(value: String)  extends ResolvedName
final case class UnresolvedColumnName(value: String) extends ResolvedName

final case class TableRef[I, R](info: I, value: R)
object TableRef {
  implicit def eqTableRef[I: Eq, R: Eq]: Eq[TableRef[I, R]] = Eq.fromUniversalEquals
  implicit def tableRefInstances[I]: Traverse[TableRef[I, ?]] = new Traverse[TableRef[I, ?]] {
    override def map[A, B](fa: TableRef[I, A])(f: A => B): TableRef[I, B] =
      fa.copy(value = f(fa.value))
    def foldLeft[A, B](fa: TableRef[I, A], b: B)(f: (B, A) => B): B = f(b, fa.value)
    def foldRight[A, B](fa: TableRef[I, A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
      Eval.defer(f(fa.value, lb))
    def traverse[G[_], A, B](fa: TableRef[I, A])(f: A => G[B])(
        implicit G: Applicative[G]): G[TableRef[I, B]] =
      Applicative[G].map(f(fa.value))(TableRef(fa.info, _))
  }
}

final case class ColumnRef[I, R](info: I, value: R)
object ColumnRef {
  implicit def eqColumnRef[I: Eq, R: Eq]: Eq[ColumnRef[I, R]] = Eq.fromUniversalEquals
  implicit def columnRefInstances[I]: Traverse[ColumnRef[I, ?]] = new Traverse[ColumnRef[I, ?]] {
    override def map[A, B](fa: ColumnRef[I, A])(f: A => B): ColumnRef[I, B] =
      fa.copy(value = f(fa.value))
    def foldLeft[A, B](fa: ColumnRef[I, A], b: B)(f: (B, A) => B): B = f(b, fa.value)
    def foldRight[A, B](fa: ColumnRef[I, A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
      Eval.defer(f(fa.value, lb))
    def traverse[G[_], A, B](fa: ColumnRef[I, A])(f: A => G[B])(
        implicit G: Applicative[G]): G[ColumnRef[I, B]] =
      Applicative[G].map(f(fa.value))(ColumnRef(fa.info, _))
  }
}

final case class UsingColumn[I, R](info: I, value: R)
object UsingColumn {
  implicit def eqUsingColumn[I: Eq, R: Eq]: Eq[UsingColumn[I, R]] = Eq.fromUniversalEquals
  implicit def columnRefInstances[I]: Traverse[UsingColumn[I, ?]] =
    new Traverse[UsingColumn[I, ?]] {
      override def map[A, B](fa: UsingColumn[I, A])(f: A => B): UsingColumn[I, B] =
        fa.copy(value = f(fa.value))
      def foldLeft[A, B](fa: UsingColumn[I, A], b: B)(f: (B, A) => B): B = f(b, fa.value)
      def foldRight[A, B](fa: UsingColumn[I, A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
        Eval.defer(f(fa.value, lb))
      def traverse[G[_], A, B](fa: UsingColumn[I, A])(f: A => G[B])(
          implicit G: Applicative[G]): G[UsingColumn[I, B]] =
        Applicative[G].map(f(fa.value))(UsingColumn(fa.info, _))
    }
}

final case class ColumnAlias[I](info: I, value: String)
final case class TableAlias[I](info: I, value: String)

// --- TREE --

sealed trait Node

final case class Query[I, R](info: I, w: Option[With[I, R]], qnw: QueryNoWith[I, R]) extends Node
object Query {
  implicit def eqQuery[I: Eq, R: Eq]: Eq[Query[I, R]] = Eq.fromUniversalEquals
  implicit def queryInstances[I]: Functor[Query[I, ?]] = new Functor[Query[I, ?]] {
    def map[A, B](fa: Query[I, A])(f: A => B): Query[I, B] =
      fa.copy(w = fa.w.map(_.map(f)), qnw = fa.qnw.map(f))
  }
}

/**
  *  Constructor for the WITH clause containing named common table expressions.
  *  Note: Despite being in the SqlBase.g4 grammar, we ignore the RECURSIVE term
  *  as it is not supported.
  */
final case class With[I, R](info: I, nqs: NonEmptyList[NamedQuery[I, R]]) extends Node
object With {
  implicit def eqWith[I: Eq, R: Eq]: Eq[With[I, R]] = Eq.fromUniversalEquals
  implicit def querySelectInstances[I]: Functor[With[I, ?]] =
    new Functor[With[I, ?]] {
      def map[A, B](fa: With[I, A])(f: A => B): With[I, B] =
        fa.copy(nqs = fa.nqs.map(_.map(f)))
    }
}

final case class QueryNoWith[I, R](info: I,
                                   qt: QueryTerm[I, R],
                                   ob: Option[OrderBy[I, R]],
                                   l: Option[Limit[I]])
    extends Node
object QueryNoWith {
  implicit def eqQueryNoWith[I: Eq, R: Eq]: Eq[QueryNoWith[I, R]] = Eq.fromUniversalEquals
  implicit def queryNoWithInstances[I]: Functor[QueryNoWith[I, ?]] =
    new Functor[QueryNoWith[I, ?]] {
      def map[A, B](fa: QueryNoWith[I, A])(f: A => B): QueryNoWith[I, B] =
        fa.copy(qt = fa.qt.map(f), ob = fa.ob.map(_.map(f)))
    }
}

final case class OrderBy[I, R](info: I, sis: NonEmptyList[SortItem[I, R]])
object OrderBy {
  implicit def eqOrderBy[I: Eq, R: Eq]: Eq[OrderBy[I, R]] = Eq.fromUniversalEquals
  implicit def queryLimitInstances[I]: Functor[OrderBy[I, ?]] = new Functor[OrderBy[I, ?]] {
    def map[A, B](fa: OrderBy[I, A])(f: A => B): OrderBy[I, B] = fa.copy(sis = fa.sis.map(_.map(f)))
  }
}

sealed trait Ordering
final case object ASC  extends Ordering
final case object DESC extends Ordering

sealed trait NullOrdering
final case object FIRST extends NullOrdering
final case object LAST  extends NullOrdering

final case class Limit[I](info: I, l: String)
object Limit {
  implicit def eqLimit[I: Eq]: Eq[Limit[I]] = Eq.fromUniversalEquals
  // TODO Override constructor and do checking on whether it is a integer value or 'all' text?
}

/**
  *  Product type for queryTerm containing a setOperation or falling through to queryPrimary
  */
sealed trait QueryTerm[I, R] extends Node
object QueryTerm {
  implicit def eqQueryTerm[I: Eq, R: Eq]: Eq[QueryTerm[I, R]] = Eq.fromUniversalEquals
  implicit def queryTermInstances[I]: Functor[QueryTerm[I, ?]] = new Functor[QueryTerm[I, ?]] {
    def map[A, B](fa: QueryTerm[I, A])(f: A => B): QueryTerm[I, B] = fa match {
      case q: QueryPrimary[I, _] => q.map(f)
      case q: SetOperation[I, _] => q.map(f)
    }
  }
}

final case class SetOperation[I, R](info: I,
                                    left: QueryTerm[I, R],
                                    op: SetOperator,
                                    sq: Option[SetQuantifier],
                                    right: QueryTerm[I, R])
    extends QueryTerm[I, R]
object SetOperation {
  implicit def eqSetOperation[I: Eq, R: Eq]: Eq[SetOperation[I, R]] = Eq.fromUniversalEquals
  implicit def querySetOperationInstances[I]: Functor[SetOperation[I, ?]] =
    new Functor[SetOperation[I, ?]] {
      def map[A, B](fa: SetOperation[I, A])(f: A => B): SetOperation[I, B] =
        fa.copy(left = fa.left.map(f), right = fa.right.map(f))
    }
}

sealed trait SetOperator
final case object INTERSECT extends SetOperator
final case object UNION     extends SetOperator
final case object EXCEPT    extends SetOperator

sealed trait SetQuantifier
final case object DISTINCT extends SetQuantifier
final case object ALL      extends SetQuantifier

/**
  *  Product type for queryPrimary
  */
sealed trait QueryPrimary[I, R] extends QueryTerm[I, R]
object QueryPrimary {
  implicit def eqQueryPrimary[I: Eq, R: Eq]: Eq[QueryPrimary[I, R]] = Eq.fromUniversalEquals
  implicit def selectionInstances[I]: Functor[QueryPrimary[I, ?]] =
    new Functor[QueryPrimary[I, ?]] {
      def map[A, B](fa: QueryPrimary[I, A])(f: A => B): QueryPrimary[I, B] = fa match {
        // TODO Can I just say fa.map(f)?
        case s: QuerySpecification[I, _] => s.map(f)
        case s: QueryPrimaryTable[I, _]  => s.map(f)
        case s: InlineTable[I, _]        => s.map(f)
        case s: SubQuery[I, _]           => s.map(f)
      }
    }
}

final case class QueryPrimaryTable[I, R](info: I, t: TableRef[I, R]) extends QueryPrimary[I, R]
object QueryPrimaryTable {
  implicit def eqQueryPrimaryTable[I: Eq, R: Eq]: Eq[QueryPrimaryTable[I, R]] =
    Eq.fromUniversalEquals
  implicit def queryQueryPrimaryTableInstances[I]: Functor[QueryPrimaryTable[I, ?]] =
    new Functor[QueryPrimaryTable[I, ?]] {
      def map[A, B](fa: QueryPrimaryTable[I, A])(f: A => B): QueryPrimaryTable[I, B] =
        fa.copy(t = fa.t.map(f))
    }
}

final case class InlineTable[I, R](info: I, vs: NonEmptyList[Expression[I, R]])
    extends QueryPrimary[I, R]
object InlineTable {
  implicit def eqInlineTable[I: Eq, R: Eq]: Eq[InlineTable[I, R]] = Eq.fromUniversalEquals
  implicit def queryInlineTableInstances[I]: Functor[InlineTable[I, ?]] =
    new Functor[InlineTable[I, ?]] {
      def map[A, B](fa: InlineTable[I, A])(f: A => B): InlineTable[I, B] =
        fa.copy(vs = fa.vs.map(_.map(f)))
    }
}

final case class SubQuery[I, R](info: I, qnw: QueryNoWith[I, R]) extends QueryPrimary[I, R]
object SubQuery {
  implicit def eqSubQuery[I: Eq, R: Eq]: Eq[SubQuery[I, R]] = Eq.fromUniversalEquals
  implicit def querySubQueryInstances[I]: Functor[SubQuery[I, ?]] = new Functor[SubQuery[I, ?]] {
    def map[A, B](fa: SubQuery[I, A])(f: A => B): SubQuery[I, B] = fa.copy(qnw = fa.qnw.map(f))
  }
}

final case class SortItem[I, R](info: I,
                                e: Expression[I, R],
                                o: Option[Ordering],
                                no: Option[NullOrdering])
    extends Node
object SortItem {
  implicit def eqSortItem[I: Eq, R: Eq]: Eq[SortItem[I, R]] = Eq.fromUniversalEquals
  implicit def queryLimitInstances[I]: Functor[SortItem[I, ?]] = new Functor[SortItem[I, ?]] {
    def map[A, B](fa: SortItem[I, A])(f: A => B): SortItem[I, B] = fa.copy(e = fa.e.map(f))
  }
}

final case class QuerySpecification[I, R](info: I,
                                          sq: Option[SetQuantifier],
                                          sis: NonEmptyList[SelectItem[I, R]],
                                          f: Option[NonEmptyList[Relation[I, R]]],
                                          w: Option[Expression[I, R]],
                                          g: Option[GroupBy[I, R]],
                                          h: Option[Expression[I, R]])
    extends QueryPrimary[I, R]
object QuerySpecification {
  implicit def eqQuerySpecification[I: Eq, R: Eq]: Eq[QuerySpecification[I, R]] =
    Eq.fromUniversalEquals
  implicit def queryLimitInstances[I]: Functor[QuerySpecification[I, ?]] =
    new Functor[QuerySpecification[I, ?]] {
      def map[A, B](fa: QuerySpecification[I, A])(f: A => B): QuerySpecification[I, B] =
        fa.copy(sis = fa.sis.map(_.map(f)),
                f = fa.f.map(_.map(_.map(f))),
                w = fa.w.map(_.map(f)),
                g = fa.g.map(_.map(f)),
                h = fa.h.map(_.map(f)))
    }
}

final case class GroupBy[I, R](info: I,
                               sq: Option[SetQuantifier],
                               ges: NonEmptyList[GroupingElement[I, R]])
    extends Node
object GroupBy {
  implicit def eqGroupBy[I: Eq, R: Eq]: Eq[GroupBy[I, R]] = Eq.fromUniversalEquals
  implicit def queryLimitInstances[I]: Functor[GroupBy[I, ?]] = new Functor[GroupBy[I, ?]] {
    def map[A, B](fa: GroupBy[I, A])(f: A => B): GroupBy[I, B] = fa.copy(ges = fa.ges.map(_.map(f)))
  }
}

sealed trait GroupingElement[I, R] extends Node
object GroupingElement {
  implicit def eqGroupingElement[I: Eq, R: Eq]: Eq[GroupingElement[I, R]] = Eq.fromUniversalEquals
  implicit def selectionInstances[I]: Functor[GroupingElement[I, ?]] =
    new Functor[GroupingElement[I, ?]] {
      def map[A, B](fa: GroupingElement[I, A])(f: A => B): GroupingElement[I, B] = fa match {
        case s: SingleGroupingSet[I, _]    => s.map(f)
        case s: Rollup[I, _]               => s.map(f)
        case s: Cube[I, _]                 => s.map(f)
        case s: MultipleGroupingSets[I, _] => s.map(f)
      }
    }
}

final case class SingleGroupingSet[I, R](info: I, g: GroupingSet[I, R])
    extends GroupingElement[I, R]
object SingleGroupingSet {
  implicit def eqSingleGroupingSet[I: Eq, R: Eq]: Eq[SingleGroupingSet[I, R]] =
    Eq.fromUniversalEquals
  implicit def queryLimitInstances[I]: Functor[SingleGroupingSet[I, ?]] =
    new Functor[SingleGroupingSet[I, ?]] {
      def map[A, B](fa: SingleGroupingSet[I, A])(f: A => B): SingleGroupingSet[I, B] =
        fa.copy(g = fa.g.map(f))
    }
}

final case class Rollup[I, R](info: I, es: List[Expression[I, R]]) extends GroupingElement[I, R]
object Rollup {
  implicit def eqRollup[I: Eq, R: Eq]: Eq[Rollup[I, R]] = Eq.fromUniversalEquals
  implicit def queryLimitInstances[I]: Functor[Rollup[I, ?]] = new Functor[Rollup[I, ?]] {
    def map[A, B](fa: Rollup[I, A])(f: A => B): Rollup[I, B] = fa.copy(es = fa.es.map(_.map(f)))
  }
}

final case class Cube[I, R](info: I, es: List[Expression[I, R]]) extends GroupingElement[I, R]
object Cube {
  implicit def eqCube[I: Eq, R: Eq]: Eq[Cube[I, R]] = Eq.fromUniversalEquals
  implicit def queryLimitInstances[I]: Functor[Cube[I, ?]] = new Functor[Cube[I, ?]] {
    def map[A, B](fa: Cube[I, A])(f: A => B): Cube[I, B] = fa.copy(es = fa.es.map(_.map(f)))
  }
}

final case class MultipleGroupingSets[I, R](info: I, gs: NonEmptyList[GroupingSet[I, R]])
    extends GroupingElement[I, R]
object MultipleGroupingSets {
  implicit def eqMultipleGroupingSets[I: Eq, R: Eq]: Eq[MultipleGroupingSets[I, R]] =
    Eq.fromUniversalEquals
  implicit def queryLimitInstances[I]: Functor[MultipleGroupingSets[I, ?]] =
    new Functor[MultipleGroupingSets[I, ?]] {
      def map[A, B](fa: MultipleGroupingSets[I, A])(f: A => B): MultipleGroupingSets[I, B] =
        fa.copy(gs = fa.gs.map(_.map(f)))
    }
}

final case class GroupingSet[I, R](info: I, es: List[Expression[I, R]]) extends Node
object GroupingSet {
  implicit def eqGroupingSet[I: Eq, R: Eq]: Eq[GroupingSet[I, R]] = Eq.fromUniversalEquals
  implicit def queryLimitInstances[I]: Functor[GroupingSet[I, ?]] = new Functor[GroupingSet[I, ?]] {
    def map[A, B](fa: GroupingSet[I, A])(f: A => B): GroupingSet[I, B] =
      fa.copy(es = fa.es.map(_.map(f)))
  }
}

final case class NamedQuery[I, R](info: I, n: String, ca: Option[ColumnAliases[I]], q: Query[I, R])
    extends Node
object NamedQuery {
  implicit def eqNamedQuery[I: Eq, R: Eq]: Eq[NamedQuery[I, R]] = Eq.fromUniversalEquals
  implicit def queryLimitInstances[I]: Functor[NamedQuery[I, ?]] = new Functor[NamedQuery[I, ?]] {
    def map[A, B](fa: NamedQuery[I, A])(f: A => B): NamedQuery[I, B] = fa.copy(q = fa.q.map(f))
  }
}

sealed trait SelectItem[I, R] extends Node
object SelectItem {
  implicit def eqSelectItem[I: Eq, R: Eq]: Eq[SelectItem[I, R]] = Eq.fromUniversalEquals
  implicit def selectionInstances[I]: Functor[SelectItem[I, ?]] = new Functor[SelectItem[I, ?]] {
    def map[A, B](fa: SelectItem[I, A])(f: A => B): SelectItem[I, B] = fa match {
      case s: SelectAll[I, _]    => s.map(f)
      case s: SelectSingle[I, _] => s.map(f)
    }
  }
}

// TODO support qualified select all
final case class SelectAll[I, R](info: I, ref: Option[TableRef[I, R]]) extends SelectItem[I, R]
object SelectAll {
  implicit def eqSelectAll[I: Eq, R: Eq]: Eq[SelectAll[I, R]] = Eq.fromUniversalEquals
  implicit def selectStarInstances[I]: Functor[SelectAll[I, ?]] = new Functor[SelectAll[I, ?]] {
    def map[A, B](fa: SelectAll[I, A])(f: A => B): SelectAll[I, B] =
      fa.copy(ref = fa.ref.map(_.map(f)))
  }
}

final case class SelectSingle[I, R](info: I, expr: Expression[I, R], alias: Option[ColumnAlias[I]])
    extends SelectItem[I, R]
object SelectSingle {
  implicit def eqSelectSingle[I: Eq, R: Eq]: Eq[SelectSingle[I, R]] = Eq.fromUniversalEquals
  implicit def selectExprInstances[I]: Functor[SelectSingle[I, ?]] =
    new Functor[SelectSingle[I, ?]] {
      def map[A, B](fa: SelectSingle[I, A])(f: A => B): SelectSingle[I, B] =
        fa.copy(expr = fa.expr.map(f))
    }
}

sealed trait Relation[I, R] extends Node
object Relation {
  implicit def eqRelation[I: Eq, R: Eq]: Eq[Relation[I, R]] = Eq.fromUniversalEquals
  implicit def relationInstance[I]: Functor[Relation[I, ?]] = new Functor[Relation[I, ?]] {
    def map[A, B](fa: Relation[I, A])(f: A => B): Relation[I, B] = fa match {
      case t: JoinRelation[I, _]    => t.map(f)
      case t: SampledRelation[I, _] => t.map(f)
    }
  }
}

// TODO Cross and Natural joins have different types in the right
final case class JoinRelation[I, R](info: I,
                                    jointype: JoinType,
                                    left: Relation[I, R],
                                    right: Relation[I, R],
                                    criteria: Option[JoinCriteria[I, R]])
    extends Relation[I, R]
object JoinRelation {
  implicit def eqJoinRelation[I: Eq, R: Eq]: Eq[JoinRelation[I, R]] = Eq.fromUniversalEquals
  implicit def tablishJoinInstances[I]: Functor[JoinRelation[I, ?]] =
    new Functor[JoinRelation[I, ?]] {
      def map[A, B](fa: JoinRelation[I, A])(f: A => B): JoinRelation[I, B] =
        fa.copy(left = fa.left.map(f),
                right = fa.right.map(f),
                criteria = fa.criteria.map(_.map(f)))
    }
}

sealed trait JoinType
final case object InnerJoin extends JoinType
final case object LeftJoin  extends JoinType
final case object RightJoin extends JoinType
final case object FullJoin  extends JoinType
final case object CrossJoin extends JoinType

sealed trait JoinCriteria[I, R]
object JoinCriteria {
  implicit def eqJoinCriteria[I: Eq, R: Eq]: Eq[JoinCriteria[I, R]] = Eq.fromUniversalEquals
  implicit def joinCriteriaInstance[I]: Functor[JoinCriteria[I, ?]] =
    new Functor[JoinCriteria[I, ?]] {
      def map[A, B](fa: JoinCriteria[I, A])(f: A => B): JoinCriteria[I, B] = fa match {
        case t: JoinOn[I, _]    => t.map(f)
        case t: JoinUsing[I, _] => t.map(f)
      }
    }
}

final case class JoinOn[I, R](info: I, be: Expression[I, R]) extends JoinCriteria[I, R]
object JoinOn {
  implicit def eqJoinOn[I: Eq, R: Eq]: Eq[JoinOn[I, R]] = Eq.fromUniversalEquals
  implicit def tablishTableInstances[I]: Functor[JoinOn[I, ?]] = new Functor[JoinOn[I, ?]] {
    def map[A, B](fa: JoinOn[I, A])(f: A => B): JoinOn[I, B] =
      fa.copy(be = fa.be.map(f))
  }
}

final case class JoinUsing[I, R](info: I, cols: NonEmptyList[UsingColumn[I, R]])
    extends JoinCriteria[I, R]
object JoinUsing {
  implicit def eqJoinUsing[I: Eq, R: Eq]: Eq[JoinUsing[I, R]] = Eq.fromUniversalEquals
  implicit def tablishTableInstances[I]: Functor[JoinUsing[I, ?]] = new Functor[JoinUsing[I, ?]] {
    def map[A, B](fa: JoinUsing[I, A])(f: A => B): JoinUsing[I, B] =
      fa.copy(cols = fa.cols.map(_.map(f)))
  }
}

final case class SampledRelation[I, R](info: I,
                                       ar: AliasedRelation[I, R],
                                       ts: Option[TableSample[I, R]])
    extends Relation[I, R]
object SampledRelation {
  implicit def eqSampledRelation[I: Eq, R: Eq]: Eq[SampledRelation[I, R]] = Eq.fromUniversalEquals
  implicit def tablishTableInstances[I]: Functor[SampledRelation[I, ?]] =
    new Functor[SampledRelation[I, ?]] {
      def map[A, B](fa: SampledRelation[I, A])(f: A => B): SampledRelation[I, B] =
        fa.copy(ar = fa.ar.map(f), ts = fa.ts.map(_.map(f)))
    }
}

final case class TableSample[I, R](info: I, st: SampleType, percentage: Expression[I, R])
object TableSample {
  implicit def eqTableSample[I: Eq, R: Eq]: Eq[TableSample[I, R]] = Eq.fromUniversalEquals
  implicit def tablishTableInstances[I]: Functor[TableSample[I, ?]] =
    new Functor[TableSample[I, ?]] {
      def map[A, B](fa: TableSample[I, A])(f: A => B): TableSample[I, B] =
        fa.copy(percentage = fa.percentage.map(f))
    }
}

sealed trait SampleType
final case object BERNOULLI extends SampleType
final case object SYSTEM    extends SampleType

final case class AliasedRelation[I, R](info: I,
                                       rp: RelationPrimary[I, R],
                                       a: Option[TableAlias[I]],
                                       colAs: Option[ColumnAliases[I]])
    extends Node
object AliasedRelation {
  implicit def eqAliasedRelation[I: Eq, R: Eq]: Eq[AliasedRelation[I, R]] = Eq.fromUniversalEquals
  implicit def aliasedRelationInstance[I]: Functor[AliasedRelation[I, ?]] =
    new Functor[AliasedRelation[I, ?]] {
      def map[A, B](fa: AliasedRelation[I, A])(f: A => B): AliasedRelation[I, B] =
        fa.copy(rp = fa.rp.map(f))
    }
}

final case class ColumnAliases[I](cols: NonEmptyList[ColumnAlias[I]]) extends Node
object ColumnAliases {
  implicit def eqColumnAliases[I: Eq]: Eq[ColumnAliases[I]] = Eq.fromUniversalEquals
}

sealed trait RelationPrimary[I, R] extends Node
object RelationPrimary {
  implicit def eqRelationPrimary[I: Eq, R: Eq]: Eq[RelationPrimary[I, R]] = Eq.fromUniversalEquals
  implicit def relationPrimaryInstance[I]: Functor[RelationPrimary[I, ?]] =
    new Functor[RelationPrimary[I, ?]] {
      def map[A, B](fa: RelationPrimary[I, A])(f: A => B): RelationPrimary[I, B] = fa match {
        case r: TableName[I, _]             => r.map(f)
        case r: SubQueryRelation[I, _]      => r.map(f)
        case r: LateralRelation[I, _]       => r.map(f)
        case r: ParenthesizedRelation[I, _] => r.map(f)
      }
    }
}
// TODO There's a lot of wrapping here
final case class TableName[I, R](info: I, r: TableRef[I, R]) extends RelationPrimary[I, R]
object TableName {
  implicit def eqTableName[I: Eq, R: Eq]: Eq[TableName[I, R]] = Eq.fromUniversalEquals
  implicit def constantExprInstances[I]: Functor[TableName[I, ?]] =
    new Functor[TableName[I, ?]] {
      def map[A, B](fa: TableName[I, A])(f: A => B): TableName[I, B] = fa.copy(r = fa.r.map(f))
    }
}

final case class SubQueryRelation[I, R](info: I, q: Query[I, R]) extends RelationPrimary[I, R]
object SubQueryRelation {
  implicit def eqSubQueryRelation[I: Eq, R: Eq]: Eq[SubQueryRelation[I, R]] = Eq.fromUniversalEquals
  implicit def constantExprInstances[I]: Functor[SubQueryRelation[I, ?]] =
    new Functor[SubQueryRelation[I, ?]] {
      def map[A, B](fa: SubQueryRelation[I, A])(f: A => B): SubQueryRelation[I, B] =
        fa.copy(q = fa.q.map(f))
    }
}

final case class LateralRelation[I, R](info: I, q: Query[I, R]) extends RelationPrimary[I, R]
object LateralRelation {
  implicit def eqLateralRelation[I: Eq, R: Eq]: Eq[LateralRelation[I, R]] = Eq.fromUniversalEquals
  implicit def constantExprInstances[I]: Functor[LateralRelation[I, ?]] =
    new Functor[LateralRelation[I, ?]] {
      def map[A, B](fa: LateralRelation[I, A])(f: A => B): LateralRelation[I, B] =
        fa.copy(q = fa.q.map(f))
    }
}

final case class ParenthesizedRelation[I, R](info: I, r: Relation[I, R])
    extends RelationPrimary[I, R]
object ParenthesizedRelation {
  implicit def eqParenthesizedRelation[I: Eq, R: Eq]: Eq[ParenthesizedRelation[I, R]] =
    Eq.fromUniversalEquals
  implicit def constantExprInstances[I]: Functor[ParenthesizedRelation[I, ?]] =
    new Functor[ParenthesizedRelation[I, ?]] {
      def map[A, B](fa: ParenthesizedRelation[I, A])(f: A => B): ParenthesizedRelation[I, B] =
        fa.copy(r = fa.r.map(f))
    }
}

sealed trait Expression[I, R] extends Node
object Expression {
  implicit def eqExpression[I: Eq, R: Eq]: Eq[Expression[I, R]] = Eq.fromUniversalEquals
  implicit def expressionInstances[I]: Functor[Expression[I, ?]] = new Functor[Expression[I, ?]] {
    def map[A, B](fa: Expression[I, A])(f: A => B): Expression[I, B] = fa match {
      case e: ConstantExpr[I, _]   => e.map(f)
      case e: ColumnExpr[I, _]     => e.map(f)
      case e: SubQueryExpr[I, _]   => e.map(f)
      case e: BooleanExpr[I, _]    => e.map(f)
      case e: ComparisonExpr[I, _] => e.map(f)
    }
  }
}

final case class ConstantExpr[I, R](info: I, col: Constant[I]) extends Expression[I, R]
object ConstantExpr {
  implicit def eqConstantExpr[I: Eq, R: Eq]: Eq[ConstantExpr[I, R]] = Eq.fromUniversalEquals
  implicit def constantExprInstances[I]: Functor[ConstantExpr[I, ?]] =
    new Functor[ConstantExpr[I, ?]] {
      def map[A, B](fa: ConstantExpr[I, A])(f: A => B): ConstantExpr[I, B] =
        ConstantExpr(fa.info, fa.col)
    }
}

final case class ColumnExpr[I, R](info: I, col: ColumnRef[I, R]) extends Expression[I, R]
object ColumnExpr {
  implicit def eqColumnExpr[I: Eq, R: Eq]: Eq[ColumnExpr[I, R]] = Eq.fromUniversalEquals
  implicit def columnExprInstances[I]: Traverse[ColumnExpr[I, ?]] = new Traverse[ColumnExpr[I, ?]] {
    override def map[A, B](fa: ColumnExpr[I, A])(f: A => B): ColumnExpr[I, B] =
      fa.copy(col = fa.col.map(f))
    def foldLeft[A, B](fa: ColumnExpr[I, A], b: B)(f: (B, A) => B): B = f(b, fa.col.value)
    def foldRight[A, B](fa: ColumnExpr[I, A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
      Eval.defer(f(fa.col.value, lb))
    def traverse[G[_], A, B](fa: ColumnExpr[I, A])(f: A => G[B])(
        implicit G: Applicative[G]): G[ColumnExpr[I, B]] =
      Applicative[G].map(f(fa.col.value)) { rn =>
        fa.map(_ => rn)
      }
  }
}

final case class SubQueryExpr[I, R](info: I, q: Query[I, R]) extends Expression[I, R]
object SubQueryExpr {
  implicit def eqSubQueryExpr[I: Eq, R: Eq]: Eq[SubQueryExpr[I, R]] = Eq.fromUniversalEquals
  implicit def subQueryExprInstances[I]: Functor[SubQueryExpr[I, ?]] =
    new Functor[SubQueryExpr[I, ?]] {
      def map[A, B](fa: SubQueryExpr[I, A])(f: A => B): SubQueryExpr[I, B] =
        fa.copy(q = fa.q.map(f))
    }
}

final case class BooleanExpr[I, R](info: I,
                                   left: Expression[I, R],
                                   op: Operator,
                                   right: Expression[I, R])
    extends Expression[I, R]
object BooleanExpr {
  implicit def eqBooleanExpr[I: Eq, R: Eq]: Eq[BooleanExpr[I, R]] = Eq.fromUniversalEquals
  implicit def booleanExprInstances[I]: Functor[BooleanExpr[I, ?]] =
    new Functor[BooleanExpr[I, ?]] {
      def map[A, B](fa: BooleanExpr[I, A])(f: A => B): BooleanExpr[I, B] =
        fa.copy(left = fa.left.map(f), right = fa.right.map(f))
    }
}

final case class ComparisonExpr[I, R](info: I,
                                      left: Expression[I, R],
                                      op: Comparison,
                                      right: Expression[I, R])
    extends Expression[I, R]
object ComparisonExpr {
  implicit def eqComparisonExpr[I: Eq, R: Eq]: Eq[ComparisonExpr[I, R]] = Eq.fromUniversalEquals
  implicit def comparisonExprInstances[I]: Functor[ComparisonExpr[I, ?]] =
    new Functor[ComparisonExpr[I, ?]] {
      def map[A, B](fa: ComparisonExpr[I, A])(f: A => B): ComparisonExpr[I, B] =
        fa.copy(left = fa.left.map(f), right = fa.right.map(f))
    }
}

sealed trait Constant[I]
final case class IntConstant[I](info: I, value: Int)        extends Constant[I]
final case class DecimalConstant[I](info: I, value: Double) extends Constant[I]
final case class DoubleConstant[I](info: I, value: Double)  extends Constant[I]
final case class StringConstant[I](info: I, value: String)  extends Constant[I]
final case class BoolConstant[I](info: I, value: Boolean)   extends Constant[I]

sealed trait Operator
final case object AND extends Operator
final case object OR  extends Operator

sealed trait Comparison
final case object EQ  extends Comparison
final case object NEQ extends Comparison
final case object LT  extends Comparison
final case object LTE extends Comparison
final case object GT  extends Comparison
final case object GTE extends Comparison
