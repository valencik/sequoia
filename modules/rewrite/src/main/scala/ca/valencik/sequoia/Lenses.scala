package ca.valencik.sequoia

import monocle.Lens
import monocle.Traversal
import cats.implicits._
import cats.Applicative

object Lenses {

  // Relation
  // -- JoinRelation: Relation, Relation
  // -- SampledRelation: AliasedRelation
  //
  // AliasedRelation: RelationPrimary
  //
  // RelationPrimary
  // -- TableName: TableRef
  // -- SubQueryRelation: Query
  // -- Unnest: List[Expression]
  // -- LateralRelation: Query
  // -- ParenthesizedRelation: Relation
  //
  // TableRef: R

  private def nameFromTable[I, R]: Lens[TableName[I, R], R] =
    Lens[TableName[I, R], R](_.ref.value)(r => tn => tn.copy(ref = tn.ref.copy(value = r)))

  private def relationFromParenthesized[I, R]: Lens[ParenthesizedRelation[I, R], Relation[I, R]] =
    Lens[ParenthesizedRelation[I, R], Relation[I, R]](_.relation)(r => pr => pr.copy(relation = r))

  private def primaryFromSampled[I, R]: Lens[SampledRelation[I, R], RelationPrimary[I, R]] =
    Lens[SampledRelation[I, R], RelationPrimary[I, R]](_.aliasedRelation.relationPrimary) {
      rp => sr => sr.copy(aliasedRelation = sr.aliasedRelation.copy(relationPrimary = rp))
    }

  def relationsFromJoin[I, R]: Traversal[JoinRelation[I, R], Relation[I, R]] = {
    def getLeft(jr: JoinRelation[I, R]): Relation[I, R]  = jr.left
    def getRight(jr: JoinRelation[I, R]): Relation[I, R] = jr.right
    Traversal.apply2(getLeft, getRight) {
      case (left, right, jr) => jr.copy(left = left, right = right)
    }
  }

  private def namesFromPrimary[I, R]: Traversal[RelationPrimary[I, R], R] =
    new Traversal[RelationPrimary[I, R], R] {
      def modifyF[F[_]: Applicative](
          f: R => F[R]
      )(s: RelationPrimary[I, R]): F[RelationPrimary[I, R]] = {
        s match {
          case tn: TableName[I, R] => nameFromTable.modifyF(f)(tn).widen
          case pr: ParenthesizedRelation[I, R] => {
            val lens: Traversal[ParenthesizedRelation[I, R], R] =
              relationFromParenthesized.composeTraversal(relationNames)
            lens.modifyF(f)(pr).widen
          }
          case _: SubQueryRelation[I, R] => ??? //TODO Requires Query support
          case _: Unnest[I, R]           => ??? //TODO Requires Expression support
          case _: LateralRelation[I, R]  => ??? //TODO Requires Query support
        }
      }
    }

  def relationNames[I, R]: Traversal[Relation[I, R], R] =
    new Traversal[Relation[I, R], R] {
      def modifyF[F[_]: Applicative](f: R => F[R])(s: Relation[I, R]): F[Relation[I, R]] = {
        s match {
          case jr: JoinRelation[I, R] => {
            val fleft: F[Relation[I, R]]  = modifyF(f)(jr.left).widen
            val fright: F[Relation[I, R]] = modifyF(f)(jr.right).widen
            fleft.product(fright).map { case (l, r) => jr.copy(left = l, right = r) }
          }
          case sr: SampledRelation[I, R] => {
            val lens: Traversal[SampledRelation[I, R], R] =
              primaryFromSampled.composeTraversal(namesFromPrimary)
            lens.modifyF(f)(sr).widen
          }
        }
      }
    }

}

object LensApp {

  import Lenses._
  def simpleRelation(name: String) =
    SampledRelation(1, AliasedRelation(2, TableName(3, TableRef(4, name)), None, None), None)

  def joinRelation(left: String, right: String) =
    JoinRelation(1, InnerJoin, simpleRelation(left), simpleRelation(right), None)

  def main(args: Array[String]): Unit = {

    val foo    = simpleRelation("foo")
    val foobar = joinRelation("foo", "bar")

    val upperFoo = relationNames[Int, String].modify(_.toUpperCase)(foo)
    println(upperFoo)

    val upperIfFoo = relationNames[Int, String].modify {
      case r => if (r.startsWith("foo")) r.toUpperCase else r
    }
    println(upperIfFoo(foobar))

  }
}
