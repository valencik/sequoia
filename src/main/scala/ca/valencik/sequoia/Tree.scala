package ca.valencik.sequoia

sealed trait Node
case class QualifiedName(name: String)
case class RawIdentifier(name: String)

sealed trait SelectItem                                                             extends Node
case class SingleColumn[A](expression: Expression[A], alias: Option[Identifier[A]]) extends SelectItem
case class AllColumns(name: Option[QualifiedName])                                  extends SelectItem

sealed trait Expression[+A] extends Node {
  def map[B](f: A => B): Expression[B]
}
final case class Identifier[A](name: A) extends Expression[A] {
  def map[B](f: A => B): Expression[B] = Identifier(f(name))
}
final case class BooleanExpression[A](left: Expression[A], op: Operator, right: Expression[A]) extends Expression[A] {
  def map[B](f: A => B): Expression[B] = BooleanExpression(left.map(f), op, right.map(f))
}
final case class ComparisonExpression[A](left: Expression[A], op: Comparison, right: Expression[A])
    extends Expression[A] {
  def map[B](f: A => B): Expression[B] = ComparisonExpression(left.map(f), op, right.map(f))
}
final case class IsNullPredicate[A](value: Expression[A]) extends Expression[A] {
  def map[B](f: A => B): Expression[B] = IsNullPredicate(value.map(f))
}
final case class IsNotNullPredicate[A](value: Expression[A]) extends Expression[A] {
  def map[B](f: A => B): Expression[B] = IsNotNullPredicate(value.map(f))
}

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

sealed trait Relation[+A] extends Node {
  def map[R](f: A => R): Relation[R]
  def toList: List[A]
  def mapJoinExpression(f: Expression[_] => Expression[_]): Relation[A] = {
    this match {
      case j: Join[A] => {
        val critereaR = j.criterea.map {
          case jo: JoinOn[_] => JoinOn(f(jo.expression))
          case other         => other
        }
        Join(j.jointype, j.left.mapJoinExpression(f), j.right.mapJoinExpression(f), critereaR)
      }
      case other => other
    }
  }
}
case class Join[A](jointype: JoinType, left: Relation[A], right: Relation[A], criterea: Option[JoinCriteria])
    extends Relation[A] {
  def map[R](f: A => R): Relation[R]                                = Join(jointype, left.map(f), right.map(f), criterea)
  def mapJoinCriteria(f: JoinCriteria => JoinCriteria): Relation[A] = Join(jointype, left, right, criterea.map(f))
  def toList: List[A]                                               = left.toList ++ right.toList
}
case class SampledRelation[A, B](relation: Relation[A], sampleType: SampleType, samplePercentage: Expression[B])
    extends Relation[A] {
  def map[R](f: A => R): Relation[R] = SampledRelation(relation.map(f), sampleType, samplePercentage)
  def toList: List[A]                = relation.toList
}
case class AliasedRelation[A, B](relation: Relation[A], alias: Identifier[B], columnNames: List[Identifier[B]])
    extends Relation[A] {
  def map[R](f: A => R): Relation[R] = AliasedRelation(relation.map(f), alias, columnNames)
  def toList: List[A]                = relation.toList
}
case class Table[A](name: A) extends Relation[A] {
  def map[R](f: A => R): Relation[R] = Table(f(name))
  def toList: List[A]                = List(name)
}

sealed trait JoinType
case object LeftJoin  extends JoinType
case object RightJoin extends JoinType
case object FullJoin  extends JoinType
case object InnerJoin extends JoinType
case object CrossJoin extends JoinType

sealed trait JoinCriteria
case object NaturalJoin extends JoinCriteria
case class JoinOn[B](expression: Expression[B]) extends JoinCriteria {
  def map[A](f: Expression[B] => A): A = f(expression)
}
case class JoinUsing[B](columns: List[Identifier[B]]) extends JoinCriteria {
  def map[A](f: Identifier[B] => A): List[A] = columns.map(f)
}

sealed trait SampleType
case object Bernoulli extends SampleType
case object System    extends SampleType

case class Select(selectItems: List[SelectItem]) extends Node {
  def show: String = s"Select: $selectItems"
}
case class From[A](relations: Option[List[Relation[A]]]) extends Node {
  def show: String = if (relations.isDefined) s"From: ${relations.get}" else ""
  def map[B](f: A => B): From[B] =
    From({
      relations.map { rs =>
        rs.map { r =>
          r.map(f)
        }
      }
    })
}
case class Where[A](expression: Option[Expression[A]]) extends Node {
  def show: String = if (expression.isDefined) s"Where: ${expression.get}" else ""
}
case class GroupingElement[+A](groupingSet: List[Expression[A]]) extends Node {
  def show: String = if (groupingSet.size >= 1) s"GroupingSet: ${groupingSet}" else ""
}

case class GroupBy[+A](groupingElements: List[GroupingElement[A]]) extends Node {
  def show: String = if (groupingElements.size >= 1) s"GroupBy: ${groupingElements}" else ""
}

case class Having[A](expression: Option[Expression[A]]) extends Node {
  def show: String = if (expression.isDefined) s"Having: ${expression.get}" else ""
}

case class QuerySpecification[R, E](
    select: Select,
    from: From[R],
    where: Where[E],
    groupBy: GroupBy[E],
    having: Having[E]
) extends Node {
  def show: String = {
    val components = List(select.show, from.show, where.show, groupBy.show, having.show)
    components.filter(_ != "").mkString("\n", "\n", "\n")
  }
}

case class OrderBy[E](items: List[SortItem[E]]) extends Node {
  def show: String = if (items.size >= 1) s"OrderBy: ${items}" else ""
}

case class Limit(value: String) extends Node {
  def show: String = if (value != "") s"Limit: ${value}" else ""
}

case class QueryNoWith[R, E](querySpecification: QuerySpecification[R, E],
                             orderBy: Option[OrderBy[E]],
                             limit: Option[Limit])
    extends Node {
  def show: String = {
    val ob         = if (orderBy.isDefined) orderBy.get.show else ""
    val l          = if (limit.isDefined) limit.get.show else ""
    val components = List(querySpecification.show, ob, l)
    components
      .filter { c =>
        c != ""
      }
      .mkString("\n", "\n", "\n")
  }
}

case class With[R, E](recursive: Boolean, queries: List[WithQuery[R, E]]) extends Node {
  def show: String = {
    val r = if (recursive) "Recursive" else ""
    s"$r With ${queries.map(_.show)}"
  }
}
case class WithQuery[R, E](name: Identifier[E], query: Query[R, E], columnNames: Option[List[RawIdentifier]])
    extends Node {
  def show: String = {
    val cn         = if (columnNames.isDefined) columnNames.get.toString else ""
    val components = List(cn, query.show)
    components
      .filter { c =>
        c != ""
      }
      .mkString(s"WithQuery $name as\n", "\n", "\n")
  }
}
case class Query[R, E](withz: Option[With[R, E]], queryNoWith: QueryNoWith[R, E]) extends Node {
  def show: String = {
    val w          = if (withz.isDefined) withz.get.show else ""
    val components = List(w, queryNoWith.show)
    components
      .filter { c =>
        c != ""
      }
      .mkString("Query\n", "\n", "\n")
  }
}
