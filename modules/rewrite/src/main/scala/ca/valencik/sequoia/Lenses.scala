package ca.valencik.sequoia

import monocle.Prism

object Lenses {

  private def anonTableName[R](rawName: R): TableName[Unit, R] =
    TableName((), TableRef((), rawName))

  private def anonRelation[R](rawName: R): Relation[Unit, R] = {
    val tn = anonTableName(rawName)
    val ar = AliasedRelation((), tn, tableAlias = None, columnAliases = None)
    SampledRelation((), aliasedRelation = ar, tableSample = None)
  }

  def tnRelation[R]: Prism[Relation[_, R], R] =
    Prism[Relation[_, R], R] {
      case jr: JoinRelation[_, R] => tnRelation.getOption(jr.left)
      case sr: SampledRelation[_, R] =>
        tnRelationPrimary.getOption(sr.aliasedRelation.relationPrimary)
    }(anonRelation)

  def tnRelationPrimary[R] =
    Prism[RelationPrimary[_, R], R] {
      case tn: TableName[_, R]             => Some(tn.ref.value)
      case _: SubQueryRelation[_, R]       => None
      case _: Unnest[_, R]                 => None
      case _: LateralRelation[_, R]        => None
      case pr: ParenthesizedRelation[_, R] => tnRelation.getOption(pr.relation)
    }(anonTableName)
}

object LensApp {

  import Lenses._

  def main(args: Array[String]): Unit = {
    val relation =
      SampledRelation(1, AliasedRelation(2, TableName(3, TableRef(4, "foo")), None, None), None)
    val x = tnRelation.getOption(relation)
    println(x)

    // TODO: We are losing all our Info params here
    val y = tnRelation[String].modify(_.toUpperCase)(relation)
    println(y)
  }
}
