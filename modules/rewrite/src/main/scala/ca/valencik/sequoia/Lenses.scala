package ca.valencik.sequoia

import monocle.Lens
import monocle.Optional
import monocle.Traversal

object Lenses {

  def tnTable[I, R]: Lens[TableName[I, R], R] =
    Lens[TableName[I, R], R](_.ref.value)(r => tn => tn.copy(ref = tn.ref.copy(value = r)))

  def rpSampledR[I, R]: Lens[SampledRelation[I, R], RelationPrimary[I, R]] =
    Lens[SampledRelation[I, R], RelationPrimary[I, R]](_.aliasedRelation.relationPrimary) {
      rp => sr => sr.copy(aliasedRelation = sr.aliasedRelation.copy(relationPrimary = rp))
    }

  def relJoinR[I, R]: Traversal[JoinRelation[I, R], Relation[I, R]] = {
    def getLeft(jr: JoinRelation[I, R]): Relation[I, R]  = jr.left
    def getRight(jr: JoinRelation[I, R]): Relation[I, R] = jr.right
    Traversal.apply2(getLeft, getRight) {
      case (left, right, jr) => jr.copy(left = left, right = right)
    }
  }

  def tnRelation[I, R]: Optional[Relation[I, R], R] =
    Optional[Relation[I, R], R] {
      case jr: JoinRelation[I, R] => tnRelation.getOption(jr.left)
      case sr: SampledRelation[I, R] =>
        tnRelationPrimary.getOption(sr.aliasedRelation.relationPrimary)
    } { rawName => relation =>
      relation match {
        case sr: SampledRelation[I, R] => {
          val rp: RelationPrimary[I, R] =
            tnRelationPrimary.set(rawName)(sr.aliasedRelation.relationPrimary)
          val newAR: AliasedRelation[I, R] = sr.aliasedRelation.copy(relationPrimary = rp)
          sr.copy(aliasedRelation = newAR)
        }
        case _ => relation
      }
    }

  def tnRelationPrimary[I, R]: Optional[RelationPrimary[I, R], R] =
    Optional[RelationPrimary[I, R], R] {
      case tn: TableName[I, R]             => Some(tn.ref.value)
      case pr: ParenthesizedRelation[I, R] => tnRelation.getOption(pr.relation)
      case _: SubQueryRelation[I, R]       => None //TODO Requires Query support
      case _: Unnest[I, R]                 => None //TODO Requires Expression support
      case _: LateralRelation[I, R]        => None //TODO Requires Query support
    } { rawName => relation =>
      relation match {
        case tn: TableName[I, R]             => tnTable.set(rawName)(tn)
        case pr: ParenthesizedRelation[I, R] => pr.copy(relation = tnRelation.set(rawName)(pr.relation))
        case _: SubQueryRelation[I, R]       => relation //TODO Requires Query support
        case _: Unnest[I, R]                 => relation //TODO Requires Expression support
        case _: LateralRelation[I, R]        => relation //TODO Requires Query support
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

    val maybeFoo  = tnRelation.getOption(foo)
    val maybeFoo2 = tnRelation.getOption(foobar)
    println(maybeFoo)
    println(maybeFoo2)

    val upperFoo = tnRelation[Int, String].modify(_.toUpperCase)(foo)
    println(upperFoo)

      // val y = relJoinR[Int, String].getAll(foobar).map(tnRelation.getOption)
    val upperIfFoo = tnRelation[Int, String].modify{
      case r => if (r.startsWith("foo")) r.toUpperCase else r
    }
    val y = relJoinR[Int, String].modify(upperIfFoo)(foobar)
    println(y)

    val firstB = relJoinR[Int, String].find{
      rel => tnRelation.getOption(rel).exists(_.startsWith("b"))
    }(foobar)
    println(firstB)
  }
}
