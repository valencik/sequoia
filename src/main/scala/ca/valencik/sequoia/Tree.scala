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
  implicit def usingColumnInstances[I]: Traverse[UsingColumn[I, ?]] =
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

final case class Identifier(value: String) extends Node

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
  implicit def withInstances[I]: Functor[With[I, ?]] =
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
  implicit def orderByInstances[I]: Functor[OrderBy[I, ?]] = new Functor[OrderBy[I, ?]] {
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
  implicit def queryPrimaryInstances[I]: Functor[QueryPrimary[I, ?]] =
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
  implicit def inlineTableInstances[I]: Functor[InlineTable[I, ?]] =
    new Functor[InlineTable[I, ?]] {
      def map[A, B](fa: InlineTable[I, A])(f: A => B): InlineTable[I, B] =
        fa.copy(vs = fa.vs.map(_.map(f)))
    }
}

final case class SubQuery[I, R](info: I, qnw: QueryNoWith[I, R]) extends QueryPrimary[I, R]
object SubQuery {
  implicit def eqSubQuery[I: Eq, R: Eq]: Eq[SubQuery[I, R]] = Eq.fromUniversalEquals
  implicit def subQueryInstances[I]: Functor[SubQuery[I, ?]] = new Functor[SubQuery[I, ?]] {
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
  implicit def sortItemInstances[I]: Functor[SortItem[I, ?]] = new Functor[SortItem[I, ?]] {
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
  implicit def querySpecificationInstances[I]: Functor[QuerySpecification[I, ?]] =
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
  implicit def groupByInstances[I]: Functor[GroupBy[I, ?]] = new Functor[GroupBy[I, ?]] {
    def map[A, B](fa: GroupBy[I, A])(f: A => B): GroupBy[I, B] = fa.copy(ges = fa.ges.map(_.map(f)))
  }
}

sealed trait GroupingElement[I, R] extends Node
object GroupingElement {
  implicit def eqGroupingElement[I: Eq, R: Eq]: Eq[GroupingElement[I, R]] = Eq.fromUniversalEquals
  implicit def groupingElementInstances[I]: Functor[GroupingElement[I, ?]] =
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
  implicit def singleGroupingSetInstances[I]: Functor[SingleGroupingSet[I, ?]] =
    new Functor[SingleGroupingSet[I, ?]] {
      def map[A, B](fa: SingleGroupingSet[I, A])(f: A => B): SingleGroupingSet[I, B] =
        fa.copy(g = fa.g.map(f))
    }
}

final case class Rollup[I, R](info: I, es: List[Expression[I, R]]) extends GroupingElement[I, R]
object Rollup {
  implicit def eqRollup[I: Eq, R: Eq]: Eq[Rollup[I, R]] = Eq.fromUniversalEquals
  implicit def rollupInstances[I]: Functor[Rollup[I, ?]] = new Functor[Rollup[I, ?]] {
    def map[A, B](fa: Rollup[I, A])(f: A => B): Rollup[I, B] = fa.copy(es = fa.es.map(_.map(f)))
  }
}

final case class Cube[I, R](info: I, es: List[Expression[I, R]]) extends GroupingElement[I, R]
object Cube {
  implicit def eqCube[I: Eq, R: Eq]: Eq[Cube[I, R]] = Eq.fromUniversalEquals
  implicit def cubeInstances[I]: Functor[Cube[I, ?]] = new Functor[Cube[I, ?]] {
    def map[A, B](fa: Cube[I, A])(f: A => B): Cube[I, B] = fa.copy(es = fa.es.map(_.map(f)))
  }
}

final case class MultipleGroupingSets[I, R](info: I, gs: NonEmptyList[GroupingSet[I, R]])
    extends GroupingElement[I, R]
object MultipleGroupingSets {
  implicit def eqMultipleGroupingSets[I: Eq, R: Eq]: Eq[MultipleGroupingSets[I, R]] =
    Eq.fromUniversalEquals
  implicit def multipleGroupingSetsInstances[I]: Functor[MultipleGroupingSets[I, ?]] =
    new Functor[MultipleGroupingSets[I, ?]] {
      def map[A, B](fa: MultipleGroupingSets[I, A])(f: A => B): MultipleGroupingSets[I, B] =
        fa.copy(gs = fa.gs.map(_.map(f)))
    }
}

final case class GroupingSet[I, R](info: I, es: List[Expression[I, R]]) extends Node
object GroupingSet {
  implicit def eqGroupingSet[I: Eq, R: Eq]: Eq[GroupingSet[I, R]] = Eq.fromUniversalEquals
  implicit def groupingSetInstances[I]: Functor[GroupingSet[I, ?]] =
    new Functor[GroupingSet[I, ?]] {
      def map[A, B](fa: GroupingSet[I, A])(f: A => B): GroupingSet[I, B] =
        fa.copy(es = fa.es.map(_.map(f)))
    }
}

final case class NamedQuery[I, R](info: I, n: String, ca: Option[ColumnAliases[I]], q: Query[I, R])
    extends Node
object NamedQuery {
  implicit def eqNamedQuery[I: Eq, R: Eq]: Eq[NamedQuery[I, R]] = Eq.fromUniversalEquals
  implicit def namedQueryInstances[I]: Functor[NamedQuery[I, ?]] = new Functor[NamedQuery[I, ?]] {
    def map[A, B](fa: NamedQuery[I, A])(f: A => B): NamedQuery[I, B] = fa.copy(q = fa.q.map(f))
  }
}

sealed trait SelectItem[I, R] extends Node
object SelectItem {
  implicit def eqSelectItem[I: Eq, R: Eq]: Eq[SelectItem[I, R]] = Eq.fromUniversalEquals
  implicit def selectItemInstances[I]: Functor[SelectItem[I, ?]] = new Functor[SelectItem[I, ?]] {
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
  implicit def selectAllInstances[I]: Functor[SelectAll[I, ?]] = new Functor[SelectAll[I, ?]] {
    def map[A, B](fa: SelectAll[I, A])(f: A => B): SelectAll[I, B] =
      fa.copy(ref = fa.ref.map(_.map(f)))
  }
}

final case class SelectSingle[I, R](info: I, expr: Expression[I, R], alias: Option[ColumnAlias[I]])
    extends SelectItem[I, R]
object SelectSingle {
  implicit def eqSelectSingle[I: Eq, R: Eq]: Eq[SelectSingle[I, R]] = Eq.fromUniversalEquals
  implicit def selectSingleInstances[I]: Functor[SelectSingle[I, ?]] =
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
  implicit def joinRelationInstances[I]: Functor[JoinRelation[I, ?]] =
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
  implicit def joinOnInstances[I]: Functor[JoinOn[I, ?]] = new Functor[JoinOn[I, ?]] {
    def map[A, B](fa: JoinOn[I, A])(f: A => B): JoinOn[I, B] =
      fa.copy(be = fa.be.map(f))
  }
}

final case class JoinUsing[I, R](info: I, cols: NonEmptyList[UsingColumn[I, R]])
    extends JoinCriteria[I, R]
object JoinUsing {
  implicit def eqJoinUsing[I: Eq, R: Eq]: Eq[JoinUsing[I, R]] = Eq.fromUniversalEquals
  implicit def joinUsingInstances[I]: Functor[JoinUsing[I, ?]] = new Functor[JoinUsing[I, ?]] {
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
  implicit def sampleRelationInstances[I]: Functor[SampledRelation[I, ?]] =
    new Functor[SampledRelation[I, ?]] {
      def map[A, B](fa: SampledRelation[I, A])(f: A => B): SampledRelation[I, B] =
        fa.copy(ar = fa.ar.map(f), ts = fa.ts.map(_.map(f)))
    }
}

final case class TableSample[I, R](info: I, st: SampleType, percentage: Expression[I, R])
object TableSample {
  implicit def eqTableSample[I: Eq, R: Eq]: Eq[TableSample[I, R]] = Eq.fromUniversalEquals
  implicit def tableSampleInstances[I]: Functor[TableSample[I, ?]] =
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
  implicit def tableNameInstances[I]: Traverse[TableName[I, ?]] = new Traverse[TableName[I, ?]] {
    override def map[A, B](fa: TableName[I, A])(f: A => B): TableName[I, B] =
      fa.copy(r = fa.r.map(f))
    def foldLeft[A, B](fa: TableName[I, A], b: B)(f: (B, A) => B): B = f(b, fa.r.value)
    def foldRight[A, B](fa: TableName[I, A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
      Eval.defer(f(fa.r.value, lb))
    def traverse[G[_], A, B](fa: TableName[I, A])(f: A => G[B])(
        implicit G: Applicative[G]): G[TableName[I, B]] =
      Applicative[G].map(f(fa.r.value)) { rn =>
        fa.map(_ => rn)
      }
  }
}

final case class SubQueryRelation[I, R](info: I, q: Query[I, R]) extends RelationPrimary[I, R]
object SubQueryRelation {
  implicit def eqSubQueryRelation[I: Eq, R: Eq]: Eq[SubQueryRelation[I, R]] = Eq.fromUniversalEquals
  implicit def subQueryRelationInstances[I]: Functor[SubQueryRelation[I, ?]] =
    new Functor[SubQueryRelation[I, ?]] {
      def map[A, B](fa: SubQueryRelation[I, A])(f: A => B): SubQueryRelation[I, B] =
        fa.copy(q = fa.q.map(f))
    }
}

final case class LateralRelation[I, R](info: I, q: Query[I, R]) extends RelationPrimary[I, R]
object LateralRelation {
  implicit def eqLateralRelation[I: Eq, R: Eq]: Eq[LateralRelation[I, R]] = Eq.fromUniversalEquals
  implicit def lateralRelationInstances[I]: Functor[LateralRelation[I, ?]] =
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
  implicit def parenthesizedRelationInstances[I]: Functor[ParenthesizedRelation[I, ?]] =
    new Functor[ParenthesizedRelation[I, ?]] {
      def map[A, B](fa: ParenthesizedRelation[I, A])(f: A => B): ParenthesizedRelation[I, B] =
        fa.copy(r = fa.r.map(f))
    }
}

sealed trait Expression[I, R] extends Node
object Expression {
  implicit def eqExpression[I: Eq, R: Eq]: Eq[Expression[I, R]] = Eq.fromUniversalEquals
  implicit def expressionInstances[I]: Functor[Expression[I, ?]] =
    new Functor[Expression[I, ?]] {
      def map[A, B](fa: Expression[I, A])(f: A => B): Expression[I, B] = fa match {
        case e: LogicalBinary[I, _]   => e.map(f)
        case e: Predicate[I, _]       => e.map(f)
        case e: ValueExpression[I, _] => e.map(f)
      }
    }
}

final case class LogicalBinary[I, R](info: I,
                                     left: Expression[I, R],
                                     op: BooleanOperator,
                                     right: Expression[I, R])
    extends Expression[I, R]
object LogicalBinary {
  implicit def eqLogicalBinary[I: Eq, R: Eq]: Eq[LogicalBinary[I, R]] = Eq.fromUniversalEquals
  implicit def LogicalBinaryInstances[I]: Functor[LogicalBinary[I, ?]] =
    new Functor[LogicalBinary[I, ?]] {
      def map[A, B](fa: LogicalBinary[I, A])(f: A => B): LogicalBinary[I, B] =
        fa.copy(left = fa.left.map(f), right = fa.right.map(f))
    }
}

sealed trait Predicate[I, R] extends Expression[I, R]
object Predicate {
  implicit def eqPredicate[I: Eq, R: Eq]: Eq[Predicate[I, R]] = Eq.fromUniversalEquals
  implicit def predicateInstances[I]: Functor[Predicate[I, ?]] = new Functor[Predicate[I, ?]] {
    def map[A, B](fa: Predicate[I, A])(f: A => B): Predicate[I, B] = fa match {
      case p: NotPredicate[I, _]         => p.map(f)
      case p: ComparisonExpr[I, _]       => p.map(f)
      case p: QuantifiedComparison[I, _] => p.map(f)
      case p: Between[I, _]              => p.map(f)
      case p: InList[I, _]               => p.map(f)
      case p: InSubQuery[I, _]           => p.map(f)
      case p: Like[I, _]                 => p.map(f)
      case p: NullPredicate[I, _]        => p.map(f)
      case p: DistinctFrom[I, _]         => p.map(f)
    }
  }
}

final case class NotPredicate[I, R](info: I, value: Expression[I, R]) extends Predicate[I, R]
object NotPredicate {
  implicit def eqNotPredicate[I: Eq, R: Eq]: Eq[NotPredicate[I, R]] = Eq.fromUniversalEquals
  implicit def nullPredicateInstances[I]: Functor[NotPredicate[I, ?]] =
    new Functor[NotPredicate[I, ?]] {
      def map[A, B](fa: NotPredicate[I, A])(f: A => B): NotPredicate[I, B] =
        fa.copy(value = fa.value.map(f))
    }
}

final case class ComparisonExpr[I, R](info: I,
                                      left: ValueExpression[I, R],
                                      op: Comparison,
                                      right: ValueExpression[I, R])
    extends Predicate[I, R]
object ComparisonExpr {
  implicit def eqComparisonExpr[I: Eq, R: Eq]: Eq[ComparisonExpr[I, R]] = Eq.fromUniversalEquals
  implicit def comparisonExprInstances[I]: Functor[ComparisonExpr[I, ?]] =
    new Functor[ComparisonExpr[I, ?]] {
      def map[A, B](fa: ComparisonExpr[I, A])(f: A => B): ComparisonExpr[I, B] =
        fa.copy(left = fa.left.map(f), right = fa.right.map(f))
    }
}

final case class QuantifiedComparison[I, R](info: I,
                                            value: ValueExpression[I, R],
                                            op: Comparison,
                                            quantifier: Quantifier,
                                            query: Query[I, R])
    extends Predicate[I, R]
object QuantifiedComparison {
  implicit def eqQuantifiedComparison[I: Eq, R: Eq]: Eq[QuantifiedComparison[I, R]] =
    Eq.fromUniversalEquals
  implicit def quantifiedComparisonInstances[I]: Functor[QuantifiedComparison[I, ?]] =
    new Functor[QuantifiedComparison[I, ?]] {
      def map[A, B](fa: QuantifiedComparison[I, A])(f: A => B): QuantifiedComparison[I, B] =
        fa.copy(value = fa.value.map(f), query = fa.query.map(f))
    }
}

sealed trait Quantifier
final case object ALLQ extends Quantifier
final case object SOME extends Quantifier
final case object ANY  extends Quantifier

final case class Between[I, R](info: I,
                               value: ValueExpression[I, R],
                               lower: ValueExpression[I, R],
                               upper: ValueExpression[I, R])
    extends Predicate[I, R]
object Between {
  implicit def eqBetween[I: Eq, R: Eq]: Eq[Between[I, R]] = Eq.fromUniversalEquals
  implicit def betweenInstances[I]: Functor[Between[I, ?]] =
    new Functor[Between[I, ?]] {
      def map[A, B](fa: Between[I, A])(f: A => B): Between[I, B] =
        fa.copy(value = fa.value.map(f), lower = fa.lower.map(f), upper = fa.upper.map(f))
    }
}

final case class InList[I, R](info: I,
                              value: ValueExpression[I, R],
                              exps: NonEmptyList[Expression[I, R]])
    extends Predicate[I, R]
object InList {
  implicit def eqInList[I: Eq, R: Eq]: Eq[InList[I, R]] = Eq.fromUniversalEquals
  implicit def inListInstances[I]: Functor[InList[I, ?]] =
    new Functor[InList[I, ?]] {
      def map[A, B](fa: InList[I, A])(f: A => B): InList[I, B] =
        fa.copy(value = fa.value.map(f), exps = fa.exps.map(_.map(f)))
    }
}

final case class InSubQuery[I, R](info: I, value: ValueExpression[I, R], query: Query[I, R])
    extends Predicate[I, R]
object InSubQuery {
  implicit def eqInSubQuery[I: Eq, R: Eq]: Eq[InSubQuery[I, R]] = Eq.fromUniversalEquals
  implicit def inSubQueryInstances[I]: Functor[InSubQuery[I, ?]] =
    new Functor[InSubQuery[I, ?]] {
      def map[A, B](fa: InSubQuery[I, A])(f: A => B): InSubQuery[I, B] =
        fa.copy(value = fa.value.map(f), query = fa.query.map(f))
    }
}

final case class Like[I, R](info: I,
                            value: ValueExpression[I, R],
                            pattern: ValueExpression[I, R],
                            escape: Option[ValueExpression[I, R]])
    extends Predicate[I, R]
object Like {
  implicit def eqLike[I: Eq, R: Eq]: Eq[Like[I, R]] = Eq.fromUniversalEquals
  implicit def likeInstances[I]: Functor[Like[I, ?]] =
    new Functor[Like[I, ?]] {
      def map[A, B](fa: Like[I, A])(f: A => B): Like[I, B] =
        fa.copy(value = fa.value.map(f),
                pattern = fa.pattern.map(f),
                escape = fa.escape.map(_.map(f)))
    }
}

final case class NullPredicate[I, R](info: I, value: ValueExpression[I, R]) extends Predicate[I, R]
object NullPredicate {
  implicit def eqNullPredicate[I: Eq, R: Eq]: Eq[NullPredicate[I, R]] = Eq.fromUniversalEquals
  implicit def nullPredicateInstances[I]: Functor[NullPredicate[I, ?]] =
    new Functor[NullPredicate[I, ?]] {
      def map[A, B](fa: NullPredicate[I, A])(f: A => B): NullPredicate[I, B] =
        fa.copy(value = fa.value.map(f))
    }
}

final case class DistinctFrom[I, R](info: I,
                                    value: ValueExpression[I, R],
                                    right: ValueExpression[I, R])
    extends Predicate[I, R]
object DistinctFrom {
  implicit def eqDistinctFrom[I: Eq, R: Eq]: Eq[DistinctFrom[I, R]] = Eq.fromUniversalEquals
  implicit def distinctFromInstances[I]: Functor[DistinctFrom[I, ?]] =
    new Functor[DistinctFrom[I, ?]] {
      def map[A, B](fa: DistinctFrom[I, A])(f: A => B): DistinctFrom[I, B] =
        fa.copy(value = fa.value.map(f), right = fa.right.map(f))
    }
}

sealed trait ValueExpression[I, R] extends Expression[I, R]
object ValueExpression {
  implicit def eqValueExpression[I: Eq, R: Eq]: Eq[ValueExpression[I, R]] = Eq.fromUniversalEquals
  implicit def valueExpressionInstances[I]: Functor[ValueExpression[I, ?]] =
    new Functor[ValueExpression[I, ?]] {
      def map[A, B](fa: ValueExpression[I, A])(f: A => B): ValueExpression[I, B] = fa match {
        case e: PrimaryExpression[I, _] => e.map(f)
        case e: ArithmeticUnary[I, _]   => e.map(f)
        case e: ArithmeticBinary[I, _]  => e.map(f)
      }
    }
}

final case class ArithmeticUnary[I, R](info: I, sign: Sign, value: ValueExpression[I, R])
    extends ValueExpression[I, R]
object ArithmeticUnary {
  implicit def eqArithmeticUnary[I: Eq, R: Eq]: Eq[ArithmeticUnary[I, R]] = Eq.fromUniversalEquals
  implicit def arithmeticUnaryInstances[I]: Functor[ArithmeticUnary[I, ?]] =
    new Functor[ArithmeticUnary[I, ?]] {
      def map[A, B](fa: ArithmeticUnary[I, A])(f: A => B): ArithmeticUnary[I, B] =
        fa.copy(value = fa.value.map(f))
    }
}

final case class ArithmeticBinary[I, R](info: I,
                                        left: ValueExpression[I, R],
                                        op: ArithmeticOperator,
                                        right: ValueExpression[I, R])
    extends ValueExpression[I, R]
object ArithmeticBinary {
  implicit def eqArithmeticBinary[I: Eq, R: Eq]: Eq[ArithmeticBinary[I, R]] = Eq.fromUniversalEquals
  implicit def arithmeticBinaryInstances[I]: Functor[ArithmeticBinary[I, ?]] =
    new Functor[ArithmeticBinary[I, ?]] {
      def map[A, B](fa: ArithmeticBinary[I, A])(f: A => B): ArithmeticBinary[I, B] =
        fa.copy(left = fa.left.map(f), right = fa.right.map(f))
    }
}

sealed trait PrimaryExpression[I, R] extends ValueExpression[I, R]
object PrimaryExpression {
  implicit def eqPrimaryExpression[I: Eq, R: Eq]: Eq[PrimaryExpression[I, R]] =
    Eq.fromUniversalEquals
  // scalastyle:off cyclomatic.complexity
  implicit def primaryExpressionInstances[I]: Functor[PrimaryExpression[I, ?]] =
    new Functor[PrimaryExpression[I, ?]] {
      def map[A, B](fa: PrimaryExpression[I, A])(f: A => B): PrimaryExpression[I, B] = fa match {
        case e: LiteralExpr[I, _]         => e.map(f)
        case e: ColumnExpr[I, _]          => e.map(f)
        case e: SubQueryExpr[I, _]        => e.map(f)
        case e: ExistsExpr[I, _]          => e.map(f)
        case e: SimpleCase[I, _]          => e.map(f)
        case e: SearchedCase[I, _]        => e.map(f)
        case e: Cast[I, _]                => e.map(f)
        case e: DereferenceExpr[I, _]     => e.map(f)
        case e: Row[I, _]                 => e.map(f)
        case e: FunctionCall[I, _]        => e.map(f)
        case e: IntervalLiteral[I, _]     => e.map(f)
        case e: SpecialDateTimeFunc[I, _] => e.map(f)
      }
    }
  // scalastyle:on cyclomatic.complexity
}

sealed trait LiteralExpr[I, R] extends PrimaryExpression[I, R]
object LiteralExpr {
  implicit def eqLiteralExpr[I: Eq, R: Eq]: Eq[LiteralExpr[I, R]] = Eq.fromUniversalEquals
  implicit def literalExprInstances[I]: Functor[LiteralExpr[I, ?]] =
    new Functor[LiteralExpr[I, ?]] {
      def map[A, B](fa: LiteralExpr[I, A])(f: A => B): LiteralExpr[I, B] =
        fa.asInstanceOf[LiteralExpr[I, B]]
    }
}
final case class DecimalLiteral[I, R](info: I, value: Double)  extends LiteralExpr[I, R]
final case class DoubleLiteral[I, R](info: I, value: Double)   extends LiteralExpr[I, R]
final case class IntLiteral[I, R](info: I, value: Long)        extends LiteralExpr[I, R]
final case class StringLiteral[I, R](info: I, value: String)   extends LiteralExpr[I, R]
final case class BooleanLiteral[I, R](info: I, value: Boolean) extends LiteralExpr[I, R]

final case class ColumnExpr[I, R](info: I, col: ColumnRef[I, R]) extends PrimaryExpression[I, R]
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

final case class SubQueryExpr[I, R](info: I, q: Query[I, R]) extends PrimaryExpression[I, R]
object SubQueryExpr {
  implicit def eqSubQueryExpr[I: Eq, R: Eq]: Eq[SubQueryExpr[I, R]] = Eq.fromUniversalEquals
  implicit def subQueryExprInstances[I]: Functor[SubQueryExpr[I, ?]] =
    new Functor[SubQueryExpr[I, ?]] {
      def map[A, B](fa: SubQueryExpr[I, A])(f: A => B): SubQueryExpr[I, B] =
        fa.copy(q = fa.q.map(f))
    }
}

final case class ExistsExpr[I, R](info: I, q: Query[I, R]) extends PrimaryExpression[I, R]
object ExistsExpr {
  implicit def eqExistsExpr[I: Eq, R: Eq]: Eq[ExistsExpr[I, R]] = Eq.fromUniversalEquals
  implicit def existsExprInstances[I]: Functor[ExistsExpr[I, ?]] =
    new Functor[ExistsExpr[I, ?]] {
      def map[A, B](fa: ExistsExpr[I, A])(f: A => B): ExistsExpr[I, B] =
        fa.copy(q = fa.q.map(f))
    }
}

final case class SimpleCase[I, R](info: I,
                                  exp: ValueExpression[I, R],
                                  whenClauses: NonEmptyList[WhenClause[I, R]],
                                  elseExpression: Option[Expression[I, R]])
    extends PrimaryExpression[I, R]
object SimpleCase {
  implicit def eqSimpleCase[I: Eq, R: Eq]: Eq[SimpleCase[I, R]] = Eq.fromUniversalEquals
  implicit def simpleCaseInstances[I]: Functor[SimpleCase[I, ?]] =
    new Functor[SimpleCase[I, ?]] {
      def map[A, B](fa: SimpleCase[I, A])(f: A => B): SimpleCase[I, B] =
        fa.copy(exp = fa.exp.map(f),
                whenClauses = fa.whenClauses.map(_.map(f)),
                elseExpression = fa.elseExpression.map(_.map(f)))
    }
}

final case class SearchedCase[I, R](info: I,
                                    whenClauses: NonEmptyList[WhenClause[I, R]],
                                    elseExpression: Option[Expression[I, R]])
    extends PrimaryExpression[I, R]
object SearchedCase {
  implicit def eqSearchedCase[I: Eq, R: Eq]: Eq[SearchedCase[I, R]] = Eq.fromUniversalEquals
  implicit def searchedCaseInstances[I]: Functor[SearchedCase[I, ?]] =
    new Functor[SearchedCase[I, ?]] {
      def map[A, B](fa: SearchedCase[I, A])(f: A => B): SearchedCase[I, B] =
        fa.copy(whenClauses = fa.whenClauses.map(_.map(f)),
                elseExpression = fa.elseExpression.map(_.map(f)))
    }
}

final case class WhenClause[I, R](info: I, condition: Expression[I, R], result: Expression[I, R])
    extends Node
object WhenClause {
  implicit def eqWhenClause[I: Eq, R: Eq]: Eq[WhenClause[I, R]] = Eq.fromUniversalEquals
  implicit def whenClauseInstances[I]: Functor[WhenClause[I, ?]] =
    new Functor[WhenClause[I, ?]] {
      def map[A, B](fa: WhenClause[I, A])(f: A => B): WhenClause[I, B] =
        fa.copy(condition = fa.condition.map(f), result = fa.result.map(f))
    }
}

final case class DereferenceExpr[I, R](info: I, base: PrimaryExpression[I, R], fieldName: String)
    extends PrimaryExpression[I, R]
object DereferenceExpr {
  implicit def eqDereferenceExpr[I: Eq, R: Eq]: Eq[DereferenceExpr[I, R]] = Eq.fromUniversalEquals
  implicit def dereferenceExprInstances[I]: Functor[DereferenceExpr[I, ?]] =
    new Functor[DereferenceExpr[I, ?]] {
      def map[A, B](fa: DereferenceExpr[I, A])(f: A => B): DereferenceExpr[I, B] =
        fa.copy(base = fa.base.map(f))
    }
}

final case class Row[I, R](info: I, exps: NonEmptyList[Expression[I, R]])
    extends PrimaryExpression[I, R]
object Row {
  implicit def eqRow[I: Eq, R: Eq]: Eq[Row[I, R]] = Eq.fromUniversalEquals
  implicit def rowInstances[I]: Functor[Row[I, ?]] =
    new Functor[Row[I, ?]] {
      def map[A, B](fa: Row[I, A])(f: A => B): Row[I, B] =
        fa.copy(exps = fa.exps.map(_.map(f)))
    }
}

// TODO Option[NonEmptyList] or just List?
final case class FunctionCall[I, R](info: I,
                                    name: String,
                                    sq: Option[SetQuantifier],
                                    exprs: List[Expression[I, R]],
                                    order: Option[OrderBy[I, R]],
                                    filter: Option[FunctionFilter[I, R]],
                                    over: Option[FunctionOver[I, R]])
    extends PrimaryExpression[I, R]
object FunctionCall {
  implicit def eqFunctionCall[I: Eq, R: Eq]: Eq[FunctionCall[I, R]] = Eq.fromUniversalEquals
  implicit def functionCallInstances[I]: Functor[FunctionCall[I, ?]] =
    new Functor[FunctionCall[I, ?]] {
      def map[A, B](fa: FunctionCall[I, A])(f: A => B): FunctionCall[I, B] =
        fa.copy(exprs = fa.exprs.map(_.map(f)),
                order = fa.order.map(_.map(f)),
                filter = fa.filter.map(_.map(f)),
                over = fa.over.map(_.map(f)))
    }
}

final case class FunctionFilter[I, R](info: I, exp: Expression[I, R]) extends Node
object FunctionFilter {
  implicit def eqFunctionFilter[I: Eq, R: Eq]: Eq[FunctionFilter[I, R]] = Eq.fromUniversalEquals
  implicit def functionFilterInstances[I]: Functor[FunctionFilter[I, ?]] =
    new Functor[FunctionFilter[I, ?]] {
      def map[A, B](fa: FunctionFilter[I, A])(f: A => B): FunctionFilter[I, B] =
        fa.copy(exp = fa.exp.map(f))
    }
}

final case class FunctionOver[I, R](info: I,
                                    partitionBy: List[Expression[I, R]],
                                    orderBy: Option[OrderBy[I, R]],
                                    window: Option[WindowFrame[I, R]])
    extends Node
object FunctionOver {
  implicit def eqFunctionOver[I: Eq, R: Eq]: Eq[FunctionOver[I, R]] = Eq.fromUniversalEquals
  implicit def functionOverInstances[I]: Functor[FunctionOver[I, ?]] =
    new Functor[FunctionOver[I, ?]] {
      def map[A, B](fa: FunctionOver[I, A])(f: A => B): FunctionOver[I, B] =
        fa.copy(partitionBy = fa.partitionBy.map(_.map(f)),
                orderBy = fa.orderBy.map(_.map(f)),
                window = fa.window.map(_.map(f)))
    }
}

final case class WindowFrame[I, R](info: I,
                                   frameType: FrameType,
                                   start: FrameBound[I, R],
                                   end: Option[FrameBound[I, R]])
    extends Node
object WindowFrame {
  implicit def eqWindowFrame[I: Eq, R: Eq]: Eq[WindowFrame[I, R]] = Eq.fromUniversalEquals
  implicit def windowFrameInstances[I]: Functor[WindowFrame[I, ?]] =
    new Functor[WindowFrame[I, ?]] {
      def map[A, B](fa: WindowFrame[I, A])(f: A => B): WindowFrame[I, B] =
        fa.copy(start = fa.start.map(f), end = fa.end.map(_.map(f)))
    }
}

sealed trait FrameBound[I, R] extends Node
object FrameBound {
  implicit def eqFrameBound[I: Eq, R: Eq]: Eq[FrameBound[I, R]] = Eq.fromUniversalEquals
  implicit def frameBoundInstances[I]: Functor[FrameBound[I, ?]] = new Functor[FrameBound[I, ?]] {
    def map[A, B](fa: FrameBound[I, A])(f: A => B): FrameBound[I, B] = fa match {
      case e: UnboundedFrame[I, _]  => e.map(f)
      case e: CurrentRowBound[I, _] => e.map(f)
      case e: BoundedFrame[I, _]    => e.map(f)
    }
  }
}

final case class UnboundedFrame[I, R](info: I, boundType: BoundType) extends FrameBound[I, R]
object UnboundedFrame {
  implicit def eqUnboundedFrame[I: Eq, R: Eq]: Eq[UnboundedFrame[I, R]] = Eq.fromUniversalEquals
  implicit def unboundedFrameInstances[I]: Functor[UnboundedFrame[I, ?]] =
    new Functor[UnboundedFrame[I, ?]] {
      def map[A, B](fa: UnboundedFrame[I, A])(f: A => B): UnboundedFrame[I, B] =
        fa.asInstanceOf[UnboundedFrame[I, B]]
    }
}

final case class CurrentRowBound[I, R](info: I) extends FrameBound[I, R]
object CurrentRowBound {
  implicit def eqCurrentRowBound[I: Eq, R: Eq]: Eq[CurrentRowBound[I, R]] = Eq.fromUniversalEquals
  implicit def currentRowBoundInstances[I]: Functor[CurrentRowBound[I, ?]] =
    new Functor[CurrentRowBound[I, ?]] {
      def map[A, B](fa: CurrentRowBound[I, A])(f: A => B): CurrentRowBound[I, B] =
        fa.asInstanceOf[CurrentRowBound[I, B]]
    }
}

// TODO maybe it's fine to use a numeric literal here instead?
final case class BoundedFrame[I, R](info: I, boundType: BoundType, exp: Expression[I, R])
    extends FrameBound[I, R]
object BoundedFrame {
  implicit def eqBoundedFrame[I: Eq, R: Eq]: Eq[BoundedFrame[I, R]] = Eq.fromUniversalEquals
  implicit def boundedFrameInstances[I]: Functor[BoundedFrame[I, ?]] =
    new Functor[BoundedFrame[I, ?]] {
      def map[A, B](fa: BoundedFrame[I, A])(f: A => B): BoundedFrame[I, B] =
        fa.copy(exp = fa.exp.map(f))
    }
}

sealed trait BoundType
final case object PRECEDING extends BoundType
final case object FOLLOWING extends BoundType

sealed trait FrameType
final case object RANGE extends FrameType
final case object ROWS  extends FrameType

final case class IntervalLiteral[I, R](info: I,
                                       sign: Sign,
                                       value: StringLiteral[I, R],
                                       from: IntervalField,
                                       to: Option[IntervalField])
    extends PrimaryExpression[I, R]
object IntervalLiteral {
  implicit def eqIntervalLiteral[I: Eq, R: Eq]: Eq[IntervalLiteral[I, R]] = Eq.fromUniversalEquals
  implicit def intervalLiteralInstances[I]: Functor[IntervalLiteral[I, ?]] =
    new Functor[IntervalLiteral[I, ?]] {
      def map[A, B](fa: IntervalLiteral[I, A])(f: A => B): IntervalLiteral[I, B] =
        fa.asInstanceOf[IntervalLiteral[I, B]]
    }
}

sealed trait IntervalField extends Node
final case object YEAR     extends IntervalField
final case object MONTH    extends IntervalField
final case object DAY      extends IntervalField
final case object HOUR     extends IntervalField
final case object MINUTE   extends IntervalField
final case object SECOND   extends IntervalField

sealed trait Sign
final case object PLUS  extends Sign
final case object MINUS extends Sign

final case class Cast[I, R](info: I, exp: Expression[I, R], `type`: String, isTry: Boolean)
    extends PrimaryExpression[I, R]
object Cast {
  implicit def eqCast[I: Eq, R: Eq]: Eq[Cast[I, R]] = Eq.fromUniversalEquals
  implicit def castInstances[I]: Functor[Cast[I, ?]] =
    new Functor[Cast[I, ?]] {
      def map[A, B](fa: Cast[I, A])(f: A => B): Cast[I, B] =
        fa.copy(exp = fa.exp.map(f))
    }
}

final case class SpecialDateTimeFunc[I, R](info: I, name: CurrentTime, precision: Option[Int])
    extends PrimaryExpression[I, R]
object SpecialDateTimeFunc {
  implicit def eqSpecialDateTimeFunc[I: Eq, R: Eq]: Eq[SpecialDateTimeFunc[I, R]] =
    Eq.fromUniversalEquals
  implicit def specialDateTimeFuncInstances[I]: Functor[SpecialDateTimeFunc[I, ?]] =
    new Functor[SpecialDateTimeFunc[I, ?]] {
      def map[A, B](fa: SpecialDateTimeFunc[I, A])(f: A => B): SpecialDateTimeFunc[I, B] =
        fa.asInstanceOf[SpecialDateTimeFunc[I, B]]
    }
}

sealed trait CurrentTime
final case object CURRENT_DATE      extends CurrentTime
final case object CURRENT_TIME      extends CurrentTime
final case object CURRENT_TIMESTAMP extends CurrentTime
final case object LOCALTIME         extends CurrentTime
final case object LOCALTIMESTAMP    extends CurrentTime

sealed trait ArithmeticOperator
final case object ADD      extends ArithmeticOperator
final case object SUBTRACT extends ArithmeticOperator
final case object MULTIPLY extends ArithmeticOperator
final case object DIVIDE   extends ArithmeticOperator
final case object MODULUS  extends ArithmeticOperator

sealed trait BooleanOperator
final case object AND extends BooleanOperator
final case object OR  extends BooleanOperator

sealed trait Comparison
final case object EQ  extends Comparison
final case object NEQ extends Comparison
final case object LT  extends Comparison
final case object LTE extends Comparison
final case object GT  extends Comparison
final case object GTE extends Comparison
