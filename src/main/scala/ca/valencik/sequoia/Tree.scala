package ca.valencik.sequoia

import cats.Functor
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
  implicit def tableRefInstances[I]: Functor[ColumnRef[I, ?]] = new Functor[ColumnRef[I, ?]] {
    def map[A, B](fa: ColumnRef[I, A])(f: A => B): ColumnRef[I, B] = fa.copy(value = f(fa.value))
  }
}

final case class ColumnAlias[I](info: I, value: String)

// --- TREE --
sealed trait Node

sealed trait Query[I, R]                                                         extends Node
final case class QueryWith[I, R](info: I, ctes: List[CTE[I, R]], q: Query[I, R]) extends Query[I, R]
final case class QuerySelect[I, R](info: I, qs: Select[I, R])                    extends Query[I, R]
final case class QueryLimit[I, R](info: I, limit: Limit[I], qs: Select[I, R])    extends Query[I, R]

final case class CTE[I, R](info: I, alias: TablishAliasT[I], cols: List[ColumnAlias[I]], q: Query[I, R]) extends Node

final case class Limit[I](info: I, value: String)

final case class Select[I, R](info: I, select: SelectCols[I, R], from: Option[From[I, R]]) extends Node

final case class SelectCols[I, R](info: I, cols: List[Selection[I, R]])

sealed trait Selection[I, R]                                            extends Node
final case class SelectStar[I, R](info: I, ref: Option[TableRef[I, R]]) extends Selection[I, R]
final case class SelectExpr[I, R](info: I, expr: Expression[I, R], alias: Option[ColumnAlias[I]])
    extends Selection[I, R]

final case class From[I, R](info: I, rels: List[Tablish[I, R]])

sealed trait Tablish[I, R]                                                                extends Node
final case class TablishTable[I, R](info: I, alias: TablishAlias[I], ref: TableRef[I, R]) extends Tablish[I, R]
object TablishTable {
  implicit def tablishTableInstances[I]: Functor[TablishTable[I, ?]] = new Functor[TablishTable[I, ?]] {
    def map[A, B](fa: TablishTable[I, A])(f: A => B): TablishTable[I, B] = fa.copy(ref = fa.ref.map(f))
  }
}
final case class TablishSubquery[I, R](info: I, alias: TablishAlias[I], q: Query[I, R]) extends Tablish[I, R]

sealed trait TablishAlias[I]
final case class TablishAliasNone[I]()                    extends TablishAlias[I]
final case class TablishAliasT[I](info: I, value: String) extends TablishAlias[I]

// Expression
sealed trait Expression[I, R]                                    extends Node
final case class ConstantExpr[I, R](info: I, col: Constant[I])   extends Expression[I, R]
final case class ColumnExpr[I, R](info: I, col: ColumnRef[I, R]) extends Expression[I, R]
final case class SubQueryExpr[I, R](info: I, q: Query[I, R])     extends Expression[I, R]

sealed trait Constant[I]
final case class IntConstant[I](info: I, value: Int)        extends Constant[I]
final case class DecimalConstant[I](info: I, value: Double) extends Constant[I]
final case class DoubleConstant[I](info: I, value: Double)  extends Constant[I]
final case class StringConstant[I](info: I, value: String)  extends Constant[I]
final case class BoolConstant[I](info: I, value: Boolean)   extends Constant[I]
