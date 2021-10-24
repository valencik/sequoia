package ca.valencik.sequoia

import cats.{Applicative, Eq, Eval, Functor, Traverse}
import cats.implicits._

final case class TableRef[I, R](info: I, value: R)
object TableRef {
  implicit def eqTableRef[I: Eq, R: Eq]: Eq[TableRef[I, R]] = Eq.fromUniversalEquals
  implicit def tableRefInstances[I]: Traverse[TableRef[I, *]] =
    new Traverse[TableRef[I, *]] {
      override def map[A, B](fa: TableRef[I, A])(f: A => B): TableRef[I, B] =
        fa.copy(value = f(fa.value))
      def foldLeft[A, B](fa: TableRef[I, A], b: B)(f: (B, A) => B): B = f(b, fa.value)
      def foldRight[A, B](fa: TableRef[I, A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
        Eval.defer(f(fa.value, lb))
      def traverse[G[_], A, B](
          fa: TableRef[I, A]
      )(f: A => G[B])(implicit G: Applicative[G]): G[TableRef[I, B]] =
        Applicative[G].map(f(fa.value))(TableRef(fa.info, _))
    }
}

final case class ColumnRef[I, R](info: I, value: R)
object ColumnRef {
  implicit def eqColumnRef[I: Eq, R: Eq]: Eq[ColumnRef[I, R]] = Eq.fromUniversalEquals
  implicit def columnRefInstances[I]: Traverse[ColumnRef[I, *]] =
    new Traverse[ColumnRef[I, *]] {
      override def map[A, B](fa: ColumnRef[I, A])(f: A => B): ColumnRef[I, B] =
        fa.copy(value = f(fa.value))
      def foldLeft[A, B](fa: ColumnRef[I, A], b: B)(f: (B, A) => B): B = f(b, fa.value)
      def foldRight[A, B](fa: ColumnRef[I, A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
        Eval.defer(f(fa.value, lb))
      def traverse[G[_], A, B](
          fa: ColumnRef[I, A]
      )(f: A => G[B])(implicit G: Applicative[G]): G[ColumnRef[I, B]] =
        Applicative[G].map(f(fa.value))(ColumnRef(fa.info, _))
    }
}

final case class UsingColumn[I, R](info: I, value: R)
object UsingColumn {
  implicit def eqUsingColumn[I: Eq, R: Eq]: Eq[UsingColumn[I, R]] = Eq.fromUniversalEquals
  implicit def usingColumnInstances[I]: Traverse[UsingColumn[I, *]] =
    new Traverse[UsingColumn[I, *]] {
      override def map[A, B](fa: UsingColumn[I, A])(f: A => B): UsingColumn[I, B] =
        fa.copy(value = f(fa.value))
      def foldLeft[A, B](fa: UsingColumn[I, A], b: B)(f: (B, A) => B): B = f(b, fa.value)
      def foldRight[A, B](fa: UsingColumn[I, A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
        Eval.defer(f(fa.value, lb))
      def traverse[G[_], A, B](
          fa: UsingColumn[I, A]
      )(f: A => G[B])(implicit G: Applicative[G]): G[UsingColumn[I, B]] =
        Applicative[G].map(f(fa.value))(UsingColumn(fa.info, _))
    }
}

final case class ColumnAlias[I](info: I, value: String)
final case class TableAlias[I](info: I, value: String)

// --- TREE --

sealed trait Node extends Product with Serializable

final case class Identifier(value: String) extends Node

final case class Query[I, R](info: I, cte: Option[With[I, R]], queryNoWith: QueryNoWith[I, R])
    extends Node

/** Constructor for the WITH clause containing named common table expressions. Note: Despite being
  * in the SqlBase.g4 grammar, we ignore the RECURSIVE term as it is not supported.
  */
final case class With[I, R](info: I, namedQueries: List[NamedQuery[I, R]]) extends Node

final case class QueryNoWith[I, R](
    info: I,
    queryTerm: QueryTerm[I, R],
    orderBy: Option[OrderBy[I, R]],
    limit: Option[Limit[I]]
) extends Node

final case class OrderBy[I, R](info: I, sortItems: List[SortItem[I, R]])

sealed trait Ordering
final case object ASC  extends Ordering
final case object DESC extends Ordering

sealed trait NullOrdering
final case object FIRST extends NullOrdering
final case object LAST  extends NullOrdering

final case class Limit[I](info: I, value: String)

/** Product type for queryTerm containing a setOperation or falling through to queryPrimary
  */
sealed trait QueryTerm[I, R] extends Node

final case class SetOperation[I, R](
    info: I,
    left: QueryTerm[I, R],
    op: SetOperator,
    setQuantifier: Option[SetQuantifier],
    right: QueryTerm[I, R]
) extends QueryTerm[I, R]

sealed trait SetOperator
final case object INTERSECT extends SetOperator
final case object UNION     extends SetOperator
final case object EXCEPT    extends SetOperator

sealed trait SetQuantifier
final case object DISTINCT extends SetQuantifier
final case object ALL      extends SetQuantifier

/** Product type for queryPrimary
  */
sealed trait QueryPrimary[I, R] extends QueryTerm[I, R]

final case class QueryPrimaryTable[I, R](info: I, table: TableRef[I, R]) extends QueryPrimary[I, R]

final case class InlineTable[I, R](info: I, values: List[Expression[I, R]])
    extends QueryPrimary[I, R]

final case class SubQuery[I, R](info: I, queryNoWith: QueryNoWith[I, R]) extends QueryPrimary[I, R]

final case class SortItem[I, R](
    info: I,
    exp: Expression[I, R],
    ordering: Option[Ordering],
    nullOrdering: Option[NullOrdering]
) extends Node

final case class QuerySpecification[I, R](
    info: I,
    setQuantifier: Option[SetQuantifier],
    selectItems: List[SelectItem[I, R]],
    from: List[Relation[I, R]],
    where: Option[Expression[I, R]],
    groupBy: Option[GroupBy[I, R]],
    having: Option[Expression[I, R]]
) extends QueryPrimary[I, R]

final case class GroupBy[I, R](
    info: I,
    setQuantifier: Option[SetQuantifier],
    groupingElements: List[GroupingElement[I, R]]
) extends Node

sealed trait GroupingElement[I, R] extends Node

final case class SingleGroupingSet[I, R](info: I, groupingSet: GroupingSet[I, R])
    extends GroupingElement[I, R]

final case class Rollup[I, R](info: I, expressions: List[Expression[I, R]])
    extends GroupingElement[I, R]

final case class Cube[I, R](info: I, expressions: List[Expression[I, R]])
    extends GroupingElement[I, R]

final case class MultipleGroupingSets[I, R](info: I, groupingSets: List[GroupingSet[I, R]])
    extends GroupingElement[I, R]

final case class GroupingSet[I, R](info: I, expressions: List[Expression[I, R]]) extends Node

final case class NamedQuery[I, R](
    info: I,
    name: String,
    columnAliases: Option[ColumnAliases[I]],
    query: Query[I, R]
) extends Node

sealed trait SelectItem[I, R] extends Node

// TODO support qualified select all
final case class SelectAll[I, R](info: I, ref: Option[TableRef[I, R]]) extends SelectItem[I, R]

final case class SelectSingle[I, R](info: I, expr: Expression[I, R], alias: Option[ColumnAlias[I]])
    extends SelectItem[I, R]

sealed trait Relation[I, R] extends Node

// TODO Cross and Natural joins have different types in the right
final case class JoinRelation[I, R](
    info: I,
    jointype: JoinType,
    left: Relation[I, R],
    right: Relation[I, R],
    criteria: Option[JoinCriteria[I, R]]
) extends Relation[I, R]

sealed trait JoinType
final case object InnerJoin extends JoinType
final case object LeftJoin  extends JoinType
final case object RightJoin extends JoinType
final case object FullJoin  extends JoinType
final case object CrossJoin extends JoinType

sealed trait JoinCriteria[I, R]

final case class JoinOn[I, R](info: I, expr: Expression[I, R]) extends JoinCriteria[I, R]

final case class JoinUsing[I, R](info: I, cols: List[UsingColumn[I, R]]) extends JoinCriteria[I, R]

final case class SampledRelation[I, R](
    info: I,
    aliasedRelation: AliasedRelation[I, R],
    tableSample: Option[TableSample[I, R]]
) extends Relation[I, R]

final case class TableSample[I, R](info: I, sampleType: SampleType, percentage: Expression[I, R])

sealed trait SampleType
final case object BERNOULLI extends SampleType
final case object SYSTEM    extends SampleType

final case class AliasedRelation[I, R](
    info: I,
    relationPrimary: RelationPrimary[I, R],
    tableAlias: Option[TableAlias[I]],
    columnAliases: Option[ColumnAliases[I]]
) extends Node

// TODO do we need Option[List[ColumnAlias]] or can we just use List[ColumnAlias]
final case class ColumnAliases[I](cols: List[ColumnAlias[I]]) extends Node

sealed trait RelationPrimary[I, R] extends Node

// TODO There's a lot of wrapping here
final case class TableName[I, R](info: I, ref: TableRef[I, R]) extends RelationPrimary[I, R]

final case class SubQueryRelation[I, R](info: I, query: Query[I, R]) extends RelationPrimary[I, R]

final case class Unnest[I, R](info: I, expressions: List[Expression[I, R]], ordinality: Boolean)
    extends RelationPrimary[I, R]

final case class LateralRelation[I, R](info: I, query: Query[I, R]) extends RelationPrimary[I, R]

final case class ParenthesizedRelation[I, R](info: I, relation: Relation[I, R])
    extends RelationPrimary[I, R]

sealed trait Expression[I, R] extends Node

final case class LogicalBinary[I, R](
    info: I,
    left: Expression[I, R],
    op: BooleanOperator,
    right: Expression[I, R]
) extends Expression[I, R]

sealed trait Predicate[I, R] extends Expression[I, R]

final case class NotPredicate[I, R](info: I, exp: Expression[I, R]) extends Predicate[I, R]

final case class ComparisonExpr[I, R](
    info: I,
    left: ValueExpression[I, R],
    op: Comparison,
    right: ValueExpression[I, R]
) extends Predicate[I, R]

final case class QuantifiedComparison[I, R](
    info: I,
    value: ValueExpression[I, R],
    op: Comparison,
    quantifier: Quantifier,
    query: Query[I, R]
) extends Predicate[I, R]

sealed trait Quantifier
final case object ALLQ extends Quantifier
final case object SOME extends Quantifier
final case object ANY  extends Quantifier

final case class Between[I, R](
    info: I,
    value: ValueExpression[I, R],
    lower: ValueExpression[I, R],
    upper: ValueExpression[I, R]
) extends Predicate[I, R]

final case class InList[I, R](
    info: I,
    value: ValueExpression[I, R],
    exps: List[Expression[I, R]]
) extends Predicate[I, R]

final case class InSubQuery[I, R](info: I, value: ValueExpression[I, R], query: Query[I, R])
    extends Predicate[I, R]

final case class Like[I, R](
    info: I,
    value: ValueExpression[I, R],
    pattern: ValueExpression[I, R],
    escape: Option[ValueExpression[I, R]]
) extends Predicate[I, R]

final case class NullPredicate[I, R](info: I, value: ValueExpression[I, R]) extends Predicate[I, R]

final case class DistinctFrom[I, R](
    info: I,
    value: ValueExpression[I, R],
    right: ValueExpression[I, R]
) extends Predicate[I, R]

sealed trait ValueExpression[I, R] extends Expression[I, R]

final case class ArithmeticUnary[I, R](info: I, sign: Sign, value: ValueExpression[I, R])
    extends ValueExpression[I, R]

final case class ArithmeticBinary[I, R](
    info: I,
    left: ValueExpression[I, R],
    op: ArithmeticOperator,
    right: ValueExpression[I, R]
) extends ValueExpression[I, R]

sealed trait PrimaryExpression[I, R] extends ValueExpression[I, R]

sealed trait LiteralExpr[I, R] extends PrimaryExpression[I, R]

final case class NullLiteral[I, R](info: I)                    extends LiteralExpr[I, R]
final case class DecimalLiteral[I, R](info: I, value: Double)  extends LiteralExpr[I, R]
final case class DoubleLiteral[I, R](info: I, value: Double)   extends LiteralExpr[I, R]
final case class IntLiteral[I, R](info: I, value: Long)        extends LiteralExpr[I, R]
final case class StringLiteral[I, R](info: I, value: String)   extends LiteralExpr[I, R]
final case class BooleanLiteral[I, R](info: I, value: Boolean) extends LiteralExpr[I, R]

final case class ColumnExpr[I, R](info: I, col: ColumnRef[I, R]) extends PrimaryExpression[I, R]

final case class SubQueryExpr[I, R](info: I, query: Query[I, R]) extends PrimaryExpression[I, R]

final case class ExistsExpr[I, R](info: I, query: Query[I, R]) extends PrimaryExpression[I, R]

final case class SimpleCase[I, R](
    info: I,
    exp: ValueExpression[I, R],
    whenClauses: List[WhenClause[I, R]],
    elseExpression: Option[Expression[I, R]]
) extends PrimaryExpression[I, R]

final case class SearchedCase[I, R](
    info: I,
    whenClauses: List[WhenClause[I, R]],
    elseExpression: Option[Expression[I, R]]
) extends PrimaryExpression[I, R]

final case class WhenClause[I, R](info: I, condition: Expression[I, R], result: Expression[I, R])
    extends Node

final case class Subscript[I, R](
    info: I,
    value: PrimaryExpression[I, R],
    index: ValueExpression[I, R]
) extends PrimaryExpression[I, R]

final case class DereferenceExpr[I, R](info: I, base: PrimaryExpression[I, R], fieldName: String)
    extends PrimaryExpression[I, R]

final case class Row[I, R](info: I, exps: List[Expression[I, R]]) extends PrimaryExpression[I, R]

final case class FunctionCall[I, R](
    info: I,
    name: String,
    sq: Option[SetQuantifier],
    exprs: List[Expression[I, R]],
    order: Option[OrderBy[I, R]],
    filter: Option[FunctionFilter[I, R]],
    over: Option[FunctionOver[I, R]]
) extends PrimaryExpression[I, R]

final case class FunctionFilter[I, R](info: I, exp: Expression[I, R]) extends Node

final case class FunctionOver[I, R](
    info: I,
    partitionBy: List[Expression[I, R]],
    orderBy: Option[OrderBy[I, R]],
    window: Option[WindowFrame[I, R]]
) extends Node

final case class WindowFrame[I, R](
    info: I,
    frameType: FrameType,
    start: FrameBound[I, R],
    end: Option[FrameBound[I, R]]
) extends Node

sealed trait FrameBound[I, R] extends Node

final case class UnboundedFrame[I, R](info: I, boundType: BoundType) extends FrameBound[I, R]

final case class CurrentRowBound[I, R](info: I) extends FrameBound[I, R]

// TODO maybe it's fine to use a numeric literal here instead?
final case class BoundedFrame[I, R](info: I, boundType: BoundType, exp: Expression[I, R])
    extends FrameBound[I, R]

sealed trait BoundType
final case object PRECEDING extends BoundType
final case object FOLLOWING extends BoundType

sealed trait FrameType
final case object RANGE extends FrameType
final case object ROWS  extends FrameType

final case class IntervalLiteral[I, R](
    info: I,
    sign: Sign,
    value: StringLiteral[I, R],
    from: IntervalField,
    to: Option[IntervalField]
) extends PrimaryExpression[I, R]

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

final case class SpecialDateTimeFunc[I, R](info: I, name: CurrentTime, precision: Option[Int])
    extends PrimaryExpression[I, R]

final case class Extract[I, R](info: I, field: String, exp: ValueExpression[I, R])
    extends PrimaryExpression[I, R]

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
