package ca.valencik.sequoia

import cats.{Functor}
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
  implicit def tableRefInstances[I]: Functor[TableRef[I, ?]] = new Functor[TableRef[I, ?]] {
    def map[A, B](fa: TableRef[I, A])(f: A => B): TableRef[I, B] = fa.copy(value = f(fa.value))
  }
}

final case class ColumnRef[I, R](info: I, value: R)
object ColumnRef {
  implicit def columnRefInstances[I]: Functor[ColumnRef[I, ?]] = new Functor[ColumnRef[I, ?]] {
    def map[A, B](fa: ColumnRef[I, A])(f: A => B): ColumnRef[I, B] = fa.copy(value = f(fa.value))
  }
}

final case class ColumnAlias[I](info: I, value: String)

// --- TREE --
sealed trait Node

sealed trait Query[I, R] extends Node
object Query {
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
  implicit def queryWithInstances[I]: Functor[QueryWith[I, ?]] = new Functor[QueryWith[I, ?]] {
    def map[A, B](fa: QueryWith[I, A])(f: A => B): QueryWith[I, B] =
      fa.copy(ctes = fa.ctes.map(_.map(f)), q = fa.q.map(f))
  }
}
final case class QuerySelect[I, R](info: I, qs: Select[I, R]) extends Query[I, R]
object QuerySelect {
  implicit def querySelectInstances[I]: Functor[QuerySelect[I, ?]] = new Functor[QuerySelect[I, ?]] {
    def map[A, B](fa: QuerySelect[I, A])(f: A => B): QuerySelect[I, B] = fa.copy(qs = fa.qs.map(f))
  }
}
final case class QueryLimit[I, R](info: I, limit: Limit[I], qs: Select[I, R]) extends Query[I, R]
object QueryLimit {
  implicit def queryLimitInstances[I]: Functor[QueryLimit[I, ?]] = new Functor[QueryLimit[I, ?]] {
    def map[A, B](fa: QueryLimit[I, A])(f: A => B): QueryLimit[I, B] = fa.copy(qs = fa.qs.map(f))
  }
}

final case class CTE[I, R](info: I, alias: TablishAliasT[I], cols: List[ColumnAlias[I]], q: Query[I, R]) extends Node
object CTE {
  implicit def cteInstances[I]: Functor[CTE[I, ?]] = new Functor[CTE[I, ?]] {
    def map[A, B](fa: CTE[I, A])(f: A => B): CTE[I, B] = fa.copy(q = fa.q.map(f))
  }
}

final case class Limit[I](info: I, value: String)

final case class Select[I, R](info: I, select: SelectCols[I, R], from: Option[From[I, R]]) extends Node
object Select {
  implicit def selectInstances[I]: Functor[Select[I, ?]] = new Functor[Select[I, ?]] {
    def map[A, B](fa: Select[I, A])(f: A => B): Select[I, B] =
      fa.copy(select = fa.select.map(f), from = fa.from.map(_.map(f)))
  }
}

final case class SelectCols[I, R](info: I, cols: List[Selection[I, R]])
object SelectCols {
  implicit def selectColsInstances[I]: Functor[SelectCols[I, ?]] = new Functor[SelectCols[I, ?]] {
    def map[A, B](fa: SelectCols[I, A])(f: A => B): SelectCols[I, B] = fa.copy(cols = fa.cols.map(_.map(f)))
  }
}

sealed trait Selection[I, R] extends Node
object Selection {
  implicit def selectionInstances[I]: Functor[Selection[I, ?]] = new Functor[Selection[I, ?]] {
    def map[A, B](fa: Selection[I, A])(f: A => B): Selection[I, B] = fa match {
      case s: SelectStar[I, _] => s.map(f)
      case s: SelectExpr[I, _] => s.map(f)
    }
  }
}
final case class SelectStar[I, R](info: I, ref: Option[TableRef[I, R]]) extends Selection[I, R]
object SelectStar {
  implicit def selectStarInstances[I]: Functor[SelectStar[I, ?]] = new Functor[SelectStar[I, ?]] {
    def map[A, B](fa: SelectStar[I, A])(f: A => B): SelectStar[I, B] = fa.copy(ref = fa.ref.map(_.map(f)))
  }
}
final case class SelectExpr[I, R](info: I, expr: Expression[I, R], alias: Option[ColumnAlias[I]])
    extends Selection[I, R]
object SelectExpr {
  implicit def selectExprInstances[I]: Functor[SelectExpr[I, ?]] = new Functor[SelectExpr[I, ?]] {
    def map[A, B](fa: SelectExpr[I, A])(f: A => B): SelectExpr[I, B] = fa.copy(expr = fa.expr.map(f))
  }
}

final case class From[I, R](info: I, rels: List[Tablish[I, R]])
object From {
  implicit def fromInstances[I]: Functor[From[I, ?]] = new Functor[From[I, ?]] {
    def map[A, B](fa: From[I, A])(f: A => B): From[I, B] = fa.copy(rels = fa.rels.map(_.map(f)))
  }
}

sealed trait Tablish[I, R] extends Node
object Tablish {
  implicit def tablishInstance[I]: Functor[Tablish[I, ?]] = new Functor[Tablish[I, ?]] {
    def map[A, B](fa: Tablish[I, A])(f: A => B): Tablish[I, B] = fa match {
      case t: TablishTable[I, _]    => t.map(f)
      case t: TablishSubquery[I, _] => t.map(f)
    }
  }
}
final case class TablishTable[I, R](info: I, alias: TablishAlias[I], ref: TableRef[I, R]) extends Tablish[I, R]
object TablishTable {
  implicit def tablishTableInstances[I]: Functor[TablishTable[I, ?]] = new Functor[TablishTable[I, ?]] {
    def map[A, B](fa: TablishTable[I, A])(f: A => B): TablishTable[I, B] = fa.copy(ref = fa.ref.map(f))
  }
}
final case class TablishSubquery[I, R](info: I, alias: TablishAlias[I], q: Query[I, R]) extends Tablish[I, R]
object TablishSubquery {
  implicit def tablishSubqueryInstances[I]: Functor[TablishSubquery[I, ?]] = new Functor[TablishSubquery[I, ?]] {
    def map[A, B](fa: TablishSubquery[I, A])(f: A => B): TablishSubquery[I, B] = fa.copy(q = fa.q.map(f))
  }
}

sealed trait TablishAlias[I]
final case class TablishAliasNone[I]()                    extends TablishAlias[I]
final case class TablishAliasT[I](info: I, value: String) extends TablishAlias[I]

// Expression
sealed trait Expression[I, R] extends Node
object Expression {
  implicit def expressionInstances[I]: Functor[Expression[I, ?]] = new Functor[Expression[I, ?]] {
    def map[A, B](fa: Expression[I, A])(f: A => B): Expression[I, B] = fa match {
      case e: ConstantExpr[I, _] => e.map(f)
      case e: ColumnExpr[I, _]   => e.map(f)
      case e: SubQueryExpr[I, _] => e.map(f)
    }
  }
}
final case class ConstantExpr[I, R](info: I, col: Constant[I]) extends Expression[I, R]
object ConstantExpr {
  implicit def constantExprInstances[I]: Functor[ConstantExpr[I, ?]] = new Functor[ConstantExpr[I, ?]] {
    def map[A, B](fa: ConstantExpr[I, A])(f: A => B): ConstantExpr[I, B] = ConstantExpr(fa.info, fa.col)
  }
}
final case class ColumnExpr[I, R](info: I, col: ColumnRef[I, R]) extends Expression[I, R]
object ColumnExpr {
  implicit def columnExprInstances[I]: Functor[ColumnExpr[I, ?]] = new Functor[ColumnExpr[I, ?]] {
    def map[A, B](fa: ColumnExpr[I, A])(f: A => B): ColumnExpr[I, B] = fa.copy(col = fa.col.map(f))
  }
}
final case class SubQueryExpr[I, R](info: I, q: Query[I, R]) extends Expression[I, R]
object SubQueryExpr {
  implicit def subQueryExprInstances[I]: Functor[SubQueryExpr[I, ?]] = new Functor[SubQueryExpr[I, ?]] {
    def map[A, B](fa: SubQueryExpr[I, A])(f: A => B): SubQueryExpr[I, B] = fa.copy(q = fa.q.map(f))
  }
}

sealed trait Constant[I]
final case class IntConstant[I](info: I, value: Int)        extends Constant[I]
final case class DecimalConstant[I](info: I, value: Double) extends Constant[I]
final case class DoubleConstant[I](info: I, value: Double)  extends Constant[I]
final case class StringConstant[I](info: I, value: String)  extends Constant[I]
final case class BoolConstant[I](info: I, value: Boolean)   extends Constant[I]
