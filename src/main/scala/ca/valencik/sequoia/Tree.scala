package ca.valencik.sequoia

import scala.language.higherKinds
import cats.{Applicative, Eq, Eval, Functor, Traverse}
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
    override def map[A, B](fa: TableRef[I, A])(f: A => B): TableRef[I, B] = fa.copy(value = f(fa.value))
    def foldLeft[A, B](fa: TableRef[I, A], b: B)(f: (B, A) => B): B       = f(b, fa.value)
    def foldRight[A, B](fa: TableRef[I, A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
      Eval.defer(f(fa.value, lb))
    def traverse[G[_], A, B](fa: TableRef[I, A])(f: A => G[B])(implicit G: Applicative[G]): G[TableRef[I, B]] =
      Applicative[G].map(f(fa.value))(TableRef(fa.info, _))
  }
}

final case class ColumnRef[I, R](info: I, value: R)
object ColumnRef {
  implicit def eqColumnRef[I: Eq, R: Eq]: Eq[ColumnRef[I, R]] = Eq.fromUniversalEquals
  implicit def columnRefInstances[I]: Traverse[ColumnRef[I, ?]] = new Traverse[ColumnRef[I, ?]] {
    override def map[A, B](fa: ColumnRef[I, A])(f: A => B): ColumnRef[I, B] = fa.copy(value = f(fa.value))
    def foldLeft[A, B](fa: ColumnRef[I, A], b: B)(f: (B, A) => B): B        = f(b, fa.value)
    def foldRight[A, B](fa: ColumnRef[I, A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
      Eval.defer(f(fa.value, lb))
    def traverse[G[_], A, B](fa: ColumnRef[I, A])(f: A => G[B])(implicit G: Applicative[G]): G[ColumnRef[I, B]] =
      Applicative[G].map(f(fa.value))(ColumnRef(fa.info, _))
  }
}

final case class UsingColumn[I, R](info: I, value: R)
object UsingColumn {
  implicit def eqUsingColumn[I: Eq, R: Eq]: Eq[UsingColumn[I, R]] = Eq.fromUniversalEquals
  implicit def columnRefInstances[I]: Traverse[UsingColumn[I, ?]] = new Traverse[UsingColumn[I, ?]] {
    override def map[A, B](fa: UsingColumn[I, A])(f: A => B): UsingColumn[I, B] = fa.copy(value = f(fa.value))
    def foldLeft[A, B](fa: UsingColumn[I, A], b: B)(f: (B, A) => B): B          = f(b, fa.value)
    def foldRight[A, B](fa: UsingColumn[I, A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
      Eval.defer(f(fa.value, lb))
    def traverse[G[_], A, B](fa: UsingColumn[I, A])(f: A => G[B])(implicit G: Applicative[G]): G[UsingColumn[I, B]] =
      Applicative[G].map(f(fa.value))(UsingColumn(fa.info, _))
  }
}

final case class ColumnAlias[I](info: I, value: String)

// --- TREE --

sealed trait Node

sealed trait Query[I, R] extends Node
object Query {
  implicit def eqQuery[I: Eq, R: Eq]: Eq[Query[I, R]] = Eq.fromUniversalEquals
  implicit def queryInstances[I]: Functor[Query[I, ?]] = new Functor[Query[I, ?]] {
    def map[A, B](fa: Query[I, A])(f: A => B): Query[I, B] = fa match {
      case q: QueryWith[I, _]   => q.map(f)
      case q: QuerySelect[I, _] => q.map(f)
      case q: QueryLimit[I, _]  => q.map(f)
    }
  }
}

final case class QueryWith[I, R](info: I, ctes: List[CTE[I, R]], q: Query[I, R]) extends Query[I, R]
object QueryWith {
  implicit def eqQueryWith[I: Eq, R: Eq]: Eq[QueryWith[I, R]] = Eq.fromUniversalEquals
  implicit def queryWithInstances[I]: Functor[QueryWith[I, ?]] = new Functor[QueryWith[I, ?]] {
    def map[A, B](fa: QueryWith[I, A])(f: A => B): QueryWith[I, B] =
      fa.copy(ctes = fa.ctes.map(_.map(f)), q = fa.q.map(f))
  }
}

final case class QuerySelect[I, R](info: I, qs: Select[I, R]) extends Query[I, R]
object QuerySelect {
  implicit def eqQuerySelect[I: Eq, R: Eq]: Eq[QuerySelect[I, R]] = Eq.fromUniversalEquals
  implicit def querySelectInstances[I]: Functor[QuerySelect[I, ?]] = new Functor[QuerySelect[I, ?]] {
    def map[A, B](fa: QuerySelect[I, A])(f: A => B): QuerySelect[I, B] = fa.copy(qs = fa.qs.map(f))
  }
}

final case class QueryLimit[I, R](info: I, limit: Limit[I], qs: Select[I, R]) extends Query[I, R]
object QueryLimit {
  implicit def eqQueryLimit[I: Eq, R: Eq]: Eq[QueryLimit[I, R]] = Eq.fromUniversalEquals
  implicit def queryLimitInstances[I]: Functor[QueryLimit[I, ?]] = new Functor[QueryLimit[I, ?]] {
    def map[A, B](fa: QueryLimit[I, A])(f: A => B): QueryLimit[I, B] = fa.copy(qs = fa.qs.map(f))
  }
}

final case class CTE[I, R](info: I, alias: TablishAliasT[I], cols: List[ColumnAlias[I]], q: Query[I, R]) extends Node
// TODO This alias might not be right
object CTE {
  implicit def eqCTE[I: Eq, R: Eq]: Eq[CTE[I, R]] = Eq.fromUniversalEquals
  implicit def cteInstances[I]: Functor[CTE[I, ?]] = new Functor[CTE[I, ?]] {
    def map[A, B](fa: CTE[I, A])(f: A => B): CTE[I, B] = fa.copy(q = fa.q.map(f))
  }
}

final case class Limit[I](info: I, value: String)

final case class Select[I, R](info: I, select: SelectCols[I, R], from: Option[From[I, R]]) extends Node
object Select {
  implicit def eqSelect[I: Eq, R: Eq]: Eq[Select[I, R]] = Eq.fromUniversalEquals
  implicit def selectInstances[I]: Functor[Select[I, ?]] = new Functor[Select[I, ?]] {
    def map[A, B](fa: Select[I, A])(f: A => B): Select[I, B] =
      fa.copy(select = fa.select.map(f), from = fa.from.map(_.map(f)))
  }
}

final case class SelectCols[I, R](info: I, cols: List[Selection[I, R]])
object SelectCols {
  implicit def eqSelectCols[I: Eq, R: Eq]: Eq[SelectCols[I, R]] = Eq.fromUniversalEquals
  implicit def selectColsInstances[I]: Functor[SelectCols[I, ?]] = new Functor[SelectCols[I, ?]] {
    def map[A, B](fa: SelectCols[I, A])(f: A => B): SelectCols[I, B] = fa.copy(cols = fa.cols.map(_.map(f)))
  }
}

sealed trait Selection[I, R] extends Node
object Selection {
  implicit def eqSelection[I: Eq, R: Eq]: Eq[Selection[I, R]] = Eq.fromUniversalEquals
  implicit def selectionInstances[I]: Functor[Selection[I, ?]] = new Functor[Selection[I, ?]] {
    def map[A, B](fa: Selection[I, A])(f: A => B): Selection[I, B] = fa match {
      case s: SelectStar[I, _] => s.map(f)
      case s: SelectExpr[I, _] => s.map(f)
    }
  }
}

final case class SelectStar[I, R](info: I, ref: Option[TableRef[I, R]]) extends Selection[I, R]
object SelectStar {
  implicit def eqSelectStar[I: Eq, R: Eq]: Eq[SelectStar[I, R]] = Eq.fromUniversalEquals
  implicit def selectStarInstances[I]: Functor[SelectStar[I, ?]] = new Functor[SelectStar[I, ?]] {
    def map[A, B](fa: SelectStar[I, A])(f: A => B): SelectStar[I, B] = fa.copy(ref = fa.ref.map(_.map(f)))
  }
}

final case class SelectExpr[I, R](info: I, expr: Expression[I, R], alias: Option[ColumnAlias[I]])
    extends Selection[I, R]
object SelectExpr {
  implicit def eqSelectExpr[I: Eq, R: Eq]: Eq[SelectExpr[I, R]] = Eq.fromUniversalEquals
  implicit def selectExprInstances[I]: Functor[SelectExpr[I, ?]] = new Functor[SelectExpr[I, ?]] {
    def map[A, B](fa: SelectExpr[I, A])(f: A => B): SelectExpr[I, B] = fa.copy(expr = fa.expr.map(f))
  }
}

final case class From[I, R](info: I, rels: List[Tablish[I, R]])
object From {
  implicit def eqFrom[I: Eq, R: Eq]: Eq[From[I, R]] = Eq.fromUniversalEquals
  implicit def fromInstances[I]: Functor[From[I, ?]] = new Functor[From[I, ?]] {
    def map[A, B](fa: From[I, A])(f: A => B): From[I, B] = fa.copy(rels = fa.rels.map(_.map(f)))
  }
}

sealed trait Tablish[I, R] extends Node
object Tablish {
  implicit def eqTablish[I: Eq, R: Eq]: Eq[Tablish[I, R]] = Eq.fromUniversalEquals
  implicit def tablishInstance[I]: Functor[Tablish[I, ?]] = new Functor[Tablish[I, ?]] {
    def map[A, B](fa: Tablish[I, A])(f: A => B): Tablish[I, B] = fa match {
      case t: TablishTable[I, _]    => t.map(f)
      case t: TablishSubquery[I, _] => t.map(f)
      case t: TablishJoin[I, _]     => t.map(f)
    }
  }
}

final case class TablishTable[I, R](info: I, alias: TablishAlias[I], ref: TableRef[I, R]) extends Tablish[I, R]
object TablishTable {
  implicit def eqTablishTable[I: Eq, R: Eq]: Eq[TablishTable[I, R]] = Eq.fromUniversalEquals
  implicit def tablishTableInstances[I]: Functor[TablishTable[I, ?]] = new Functor[TablishTable[I, ?]] {
    def map[A, B](fa: TablishTable[I, A])(f: A => B): TablishTable[I, B] = fa.copy(ref = fa.ref.map(f))
  }
}

final case class TablishSubquery[I, R](info: I, alias: TablishAlias[I], q: Query[I, R]) extends Tablish[I, R]
object TablishSubquery {
  implicit def eqTablishSubquery[I: Eq, R: Eq]: Eq[TablishSubquery[I, R]] = Eq.fromUniversalEquals
  implicit def tablishSubqueryInstances[I]: Functor[TablishSubquery[I, ?]] = new Functor[TablishSubquery[I, ?]] {
    def map[A, B](fa: TablishSubquery[I, A])(f: A => B): TablishSubquery[I, B] = fa.copy(q = fa.q.map(f))
  }
}

final case class TablishJoin[I, R](info: I,
                                   jointype: JoinType,
                                   left: Tablish[I, R],
                                   right: Tablish[I, R],
                                   criteria: Option[JoinCriteria[I, R]])
    extends Tablish[I, R]
object TablishJoin {
  implicit def eqTablishJoin[I: Eq, R: Eq]: Eq[TablishJoin[I, R]] = Eq.fromUniversalEquals
  implicit def tablishJoinInstances[I]: Functor[TablishJoin[I, ?]] = new Functor[TablishJoin[I, ?]] {
    def map[A, B](fa: TablishJoin[I, A])(f: A => B): TablishJoin[I, B] =
      fa.copy(left = fa.left.map(f), right = fa.right.map(f), criteria = fa.criteria.map(_.map(f)))
  }
}

sealed trait JoinType
final case object LeftJoin  extends JoinType
final case object RightJoin extends JoinType
final case object FullJoin  extends JoinType
final case object InnerJoin extends JoinType
final case object CrossJoin extends JoinType

sealed trait JoinCriteria[I, R]
object JoinCriteria {
  implicit def eqJoinCriteria[I: Eq, R: Eq]: Eq[JoinCriteria[I, R]] = Eq.fromUniversalEquals
  implicit def tablishInstance[I]: Functor[JoinCriteria[I, ?]] = new Functor[JoinCriteria[I, ?]] {
    def map[A, B](fa: JoinCriteria[I, A])(f: A => B): JoinCriteria[I, B] = fa match {
      case t: NaturalJoin[I, _] => t.map(f)
      case t: JoinOn[I, _]      => t.map(f)
      case t: JoinUsing[I, _]   => t.map(f)
    }
  }
}

final case class NaturalJoin[I, R](info: I) extends JoinCriteria[I, R]
object NaturalJoin {
  implicit def eqNaturalJoin[I: Eq, R: Eq]: Eq[NaturalJoin[I, R]] = Eq.fromUniversalEquals
  implicit def tablishTableInstances[I]: Functor[NaturalJoin[I, ?]] = new Functor[NaturalJoin[I, ?]] {
    def map[A, B](fa: NaturalJoin[I, A])(f: A => B): NaturalJoin[I, B] = NaturalJoin(fa.info)
  }
}

final case class JoinOn[I, R](info: I, expression: Expression[I, R]) extends JoinCriteria[I, R]
object JoinOn {
  implicit def eqJoinOn[I: Eq, R: Eq]: Eq[JoinOn[I, R]] = Eq.fromUniversalEquals
  implicit def tablishTableInstances[I]: Functor[JoinOn[I, ?]] = new Functor[JoinOn[I, ?]] {
    def map[A, B](fa: JoinOn[I, A])(f: A => B): JoinOn[I, B] = fa.copy(expression = fa.expression.map(f))
  }
}

final case class JoinUsing[I, R](info: I, cols: List[UsingColumn[I, R]]) extends JoinCriteria[I, R]
object JoinUsing {
  implicit def eqJoinUsing[I: Eq, R: Eq]: Eq[JoinUsing[I, R]] = Eq.fromUniversalEquals
  implicit def tablishTableInstances[I]: Functor[JoinUsing[I, ?]] = new Functor[JoinUsing[I, ?]] {
    def map[A, B](fa: JoinUsing[I, A])(f: A => B): JoinUsing[I, B] = fa.copy(cols = fa.cols.map(_.map(f)))
  }
}

sealed trait TablishAlias[I]
final case class TablishAliasNone[I]()                    extends TablishAlias[I]
final case class TablishAliasT[I](info: I, value: String) extends TablishAlias[I]

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
  implicit def constantExprInstances[I]: Functor[ConstantExpr[I, ?]] = new Functor[ConstantExpr[I, ?]] {
    def map[A, B](fa: ConstantExpr[I, A])(f: A => B): ConstantExpr[I, B] = ConstantExpr(fa.info, fa.col)
  }
}

final case class ColumnExpr[I, R](info: I, col: ColumnRef[I, R]) extends Expression[I, R]
object ColumnExpr {
  implicit def eqColumnExpr[I: Eq, R: Eq]: Eq[ColumnExpr[I, R]] = Eq.fromUniversalEquals
  implicit def columnExprInstances[I]: Traverse[ColumnExpr[I, ?]] = new Traverse[ColumnExpr[I, ?]] {
    override def map[A, B](fa: ColumnExpr[I, A])(f: A => B): ColumnExpr[I, B] = fa.copy(col = fa.col.map(f))
    def foldLeft[A, B](fa: ColumnExpr[I, A], b: B)(f: (B, A) => B): B         = f(b, fa.col.value)
    def foldRight[A, B](fa: ColumnExpr[I, A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
      Eval.defer(f(fa.col.value, lb))
    def traverse[G[_], A, B](fa: ColumnExpr[I, A])(f: A => G[B])(implicit G: Applicative[G]): G[ColumnExpr[I, B]] =
      Applicative[G].map(f(fa.col.value)) { rn =>
        fa.map(_ => rn)
      }
  }
}

final case class SubQueryExpr[I, R](info: I, q: Query[I, R]) extends Expression[I, R]
object SubQueryExpr {
  implicit def eqSubQueryExpr[I: Eq, R: Eq]: Eq[SubQueryExpr[I, R]] = Eq.fromUniversalEquals
  implicit def subQueryExprInstances[I]: Functor[SubQueryExpr[I, ?]] = new Functor[SubQueryExpr[I, ?]] {
    def map[A, B](fa: SubQueryExpr[I, A])(f: A => B): SubQueryExpr[I, B] = fa.copy(q = fa.q.map(f))
  }
}

final case class BooleanExpr[I, R](info: I, left: Expression[I, R], op: Operator, right: Expression[I, R])
    extends Expression[I, R]
object BooleanExpr {
  implicit def eqBooleanExpr[I: Eq, R: Eq]: Eq[BooleanExpr[I, R]] = Eq.fromUniversalEquals
  implicit def booleanExprInstances[I]: Functor[BooleanExpr[I, ?]] = new Functor[BooleanExpr[I, ?]] {
    def map[A, B](fa: BooleanExpr[I, A])(f: A => B): BooleanExpr[I, B] =
      fa.copy(left = fa.left.map(f), right = fa.right.map(f))
  }
}

final case class ComparisonExpr[I, R](info: I, left: Expression[I, R], op: Comparison, right: Expression[I, R])
    extends Expression[I, R]
object ComparisonExpr {
  implicit def eqComparisonExpr[I: Eq, R: Eq]: Eq[ComparisonExpr[I, R]] = Eq.fromUniversalEquals
  implicit def comparisonExprInstances[I]: Functor[ComparisonExpr[I, ?]] = new Functor[ComparisonExpr[I, ?]] {
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
