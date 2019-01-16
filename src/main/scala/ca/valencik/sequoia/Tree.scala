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

final case class ColumnAlias[I](info: I, value: String)

final case class TableRef[R, I](info: I, value: R)
object TableRef {
  implicit def tableRefInstances[I]: Functor[TableRef[?, I]] = new Functor[TableRef[?, I]] {
    def map[A, B](fa: TableRef[A, I])(f: A => B): TableRef[B, I] = fa.copy(value = f(fa.value))
  }
}
final case class ColumnRef[R, I](info: I, value: R)
object ColumnRef {
  implicit def tableRefInstances[I]: Functor[ColumnRef[?, I]] = new Functor[ColumnRef[?, I]] {
    def map[A, B](fa: ColumnRef[A, I])(f: A => B): ColumnRef[B, I] = fa.copy(value = f(fa.value))
  }
}


// --- TREE --
sealed trait Node

sealed trait Query[R, I]                                                         extends Node
final case class QueryWith[R, I](info: I, ctes: List[CTE[R, I]], q: Query[R, I]) extends Query[R, I]
final case class QuerySelect[R, I](info: I, qs: Select[R, I])                    extends Query[R, I]
final case class QueryLimit[R, I](info: I, limit: Limit[I], qs: Select[R, I])    extends Query[R, I]

final case class CTE[R, I](info: I, alias: TablishAliasT[I], cols: List[ColumnAlias[I]], q: Query[R, I]) extends Node

final case class Limit[I](info: I, value: String)

final case class Select[R, I](info: I, select: SelectCols[R, I], from: Option[From[R, I]]) extends Node

final case class SelectCols[R, I](info: I, cols: List[Selection[R, I]])

sealed trait Selection[R, I]                                            extends Node
final case class SelectStar[R, I](info: I, ref: Option[TableRef[R, I]]) extends Selection[R, I]
final case class SelectExpr[R, I](info: I, expr: Expression[R, I], alias: Option[ColumnAlias[I]])
    extends Selection[R, I]

final case class From[R, I](info: I, rels: List[Tablish[R, I]])

sealed trait Tablish[R, I]                                                                extends Node
final case class TablishTable[R, I](info: I, alias: TablishAlias[I], ref: TableRef[R, I]) extends Tablish[R, I]
object TablishTable {
  implicit def tablishTableInstances[I]: Functor[TablishTable[?, I]] = new Functor[TablishTable[?, I]] {
    def map[A, B](fa: TablishTable[A, I])(f: A => B): TablishTable[B, I] = fa.copy(ref = fa.ref.map(f))
  }
}
final case class TablishSubquery[R, I](info: I, alias: TablishAlias[I], q: Query[R, I])   extends Tablish[R, I]

sealed trait TablishAlias[I]
final case class TablishAliasNone[I]()                    extends TablishAlias[I]
final case class TablishAliasT[I](info: I, value: String) extends TablishAlias[I]


// Expression
sealed trait Expression[R, I]                                    extends Node
final case class ConstantExpr[R, I](info: I, col: Constant[I])   extends Expression[R, I]
final case class ColumnExpr[R, I](info: I, col: ColumnRef[R, I]) extends Expression[R, I]
final case class SubQueryExpr[R, I](info: I, q: Query[R, I])     extends Expression[R, I]

sealed trait Constant[I]
final case class IntConstant[I](info: I, value: Int)        extends Constant[I]
final case class DecimalConstant[I](info: I, value: Double) extends Constant[I]
final case class DoubleConstant[I](info: I, value: Double)  extends Constant[I]
final case class StringConstant[I](info: I, value: String)  extends Constant[I]
final case class BoolConstant[I](info: I, value: Boolean)   extends Constant[I]
