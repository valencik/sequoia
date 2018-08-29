package ca.valencik.bigsqlparse

sealed trait Node
case class QualifiedName(name: String)
case class RawIdentifier(name: String)

sealed trait SelectItem                                                             extends Node
case class SingleColumn[A](expression: Expression[A], alias: Option[Identifier[A]]) extends SelectItem
case class AllColumns(name: Option[QualifiedName])                                  extends SelectItem

sealed trait Expression[+A]                                                                    extends Node
final case class Identifier[A](name: A)                                                        extends Expression[A]
final case class BooleanExpression[A](left: Expression[A], op: Operator, right: Expression[A]) extends Expression[A]
final case class ComparisonExpression[A](left: Expression[A], op: Comparison, right: Expression[A])
    extends Expression[A]
final case class IsNullPredicate[A](value: Expression[A])    extends Expression[A]
final case class IsNotNullPredicate[A](value: Expression[A]) extends Expression[A]

sealed trait Operator
case object AND extends Operator
case object OR  extends Operator

sealed trait Comparison
case object EQ  extends Comparison
case object NEQ extends Comparison
case object LT  extends Comparison
case object LTE extends Comparison
case object GT  extends Comparison
case object GTE extends Comparison

case class SortItem[A](expression: Expression[A], ordering: Option[SortOrdering], nullOrdering: Option[NullOrdering])
    extends Node

sealed trait SortOrdering
case object ASC  extends SortOrdering
case object DESC extends SortOrdering

sealed trait NullOrdering
case object FIRST extends NullOrdering
case object LAST  extends NullOrdering

sealed trait Relation[+A] extends Node
case class Join[A](jointype: JoinType, left: Relation[A], right: Relation[A], criterea: Option[JoinCriteria])
    extends Relation[A]
case class SampledRelation[A, B](relation: Relation[A], sampleType: SampleType, samplePercentage: Expression[B])
    extends Relation[A]
case class AliasedRelation[A, B](relation: Relation[A], alias: Identifier[B], columnNames: List[Identifier[B]])
    extends Relation[A]
case class Table[A](name: A) extends Relation[A]

sealed trait JoinType
case object LeftJoin  extends JoinType
case object RightJoin extends JoinType
case object FullJoin  extends JoinType
case object InnerJoin extends JoinType
case object CrossJoin extends JoinType

sealed trait JoinCriteria
case object NaturalJoin                               extends JoinCriteria
case class JoinOn[B](expression: Expression[B])       extends JoinCriteria {
  def map[A](f: Expression[B] => A): A = f(expression)
}
case class JoinUsing[B](columns: List[Identifier[B]]) extends JoinCriteria {
  def map[A](f: Identifier[B] => A): List[A] = columns.map(f)
}

sealed trait SampleType
case object Bernoulli extends SampleType
case object System    extends SampleType

case class Select(selectItems: List[SelectItem])                   extends Node
case class From[A](relations: List[Relation[A]])                   extends Node
case class Where[A](expression: Option[Expression[A]])             extends Node
case class GroupingElement[+A](groupingSet: List[Expression[A]])   extends Node
case class GroupBy[+A](groupingElements: List[GroupingElement[A]]) extends Node
case class Having[A](expression: Option[Expression[A]])            extends Node
case class QuerySpecification[R, E](
    select: Select,
    from: From[R],
    where: Where[E],
    groupBy: GroupBy[E],
    having: Having[E]
) extends Node

case class OrderBy[E](items: List[SortItem[E]]) extends Node
case class Limit(value: String)                 extends Node
case class QueryNoWith[R, E](querySpecification: Option[QuerySpecification[R, E]],
                             orderBy: Option[OrderBy[E]],
                             limit: Option[Limit])
    extends Node
