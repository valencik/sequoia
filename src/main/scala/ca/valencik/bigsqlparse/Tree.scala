package ca.valencik.bigsqlparse

sealed trait Node
case class Name(text: String)                    extends Node with Relation
case class Select(selectItems: List[SelectItem]) extends Node

sealed trait SelectItem                                                    extends Node
case class SingleColumn(expression: Expression, alias: Option[Identifier]) extends SelectItem
case class AllColumns(name: Option[QualifiedName])                         extends SelectItem

sealed trait Relation
case class From(relations: List[Relation]) extends Node

sealed trait Expression
case class Identifier(name: String)                                                  extends Node with Expression
case class QualifiedName(name: String)                                               extends Node with Expression
case class BooleanExpression(left: Expression, op: Operator, right: Expression)      extends Node with Expression
case class ComparisonExpression(left: Expression, op: Comparison, right: Expression) extends Node with Expression
case class IsNullPredicate(value: Expression)                                        extends Node with Expression
case class IsNotNullPredicate(value: Expression)                                     extends Node with Expression

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

case class SortItem(expression: Expression, ordering: Option[SortOrdering], nullOrdering: Option[NullOrdering])
    extends Node

sealed trait SortOrdering
case object ASC  extends SortOrdering
case object DESC extends SortOrdering

sealed trait NullOrdering
case object FIRST extends NullOrdering
case object LAST  extends NullOrdering

case class Where(expression: Option[Expression])            extends Node
case class GroupingElement(groupingSet: List[Expression])   extends Node
case class GroupBy(groupingElements: List[GroupingElement]) extends Node
case class Having(expression: Option[Expression])           extends Node
case class QuerySpecification(
    select: Select,
    from: From,
    where: Where,
    groupBy: GroupBy,
    having: Having
) extends Node

case class OrderBy(items: List[SortItem]) extends Node
case class Limit(value: String)           extends Node
case class QueryNoWith(querySpecification: Option[QuerySpecification], orderBy: Option[OrderBy], limit: Option[Limit])
    extends Node

sealed trait JoinType
case object LeftJoin  extends JoinType
case object RightJoin extends JoinType
case object FullJoin  extends JoinType
case object InnerJoin extends JoinType
case object CrossJoin extends JoinType
sealed trait JoinCriteria
case object NaturalJoin                         extends JoinCriteria
case class JoinOn(expression: Expression)       extends JoinCriteria
case class JoinUsing(columns: List[Identifier]) extends JoinCriteria
case class Join(jointype: JoinType, left: Relation, right: Relation, criterea: Option[JoinCriteria])
    extends Node
    with Relation
case class SampledRelation(text: String) extends Node with Relation
