package ca.valencik.sequoia

import cats.{Functor, Traverse}
import cats.tests.CatsSuite
import ca.valencik.sequoia.arbitrary._
import org.scalacheck.Arbitrary.{arbitrary => getArbitrary}

import cats.laws.discipline.{FunctorTests, SerializableTests, TraverseTests}

class TreeLawTests extends CatsSuite {
  checkAll("TableRef[Int, Int] with Option",
           TraverseTests[TableRef[Int, ?]].traverse[Int, Int, Int, Set[Int], Option, Option])
  checkAll("Traverse[TableRef[Int, ?]]", SerializableTests.serializable(Traverse[TableRef[Int, ?]]))

  checkAll("ColumnRef[Int, Int] with Option",
           TraverseTests[ColumnRef[Int, ?]].traverse[Int, Int, Int, Set[Int], Option, Option])
  checkAll("Traverse[ColumnRef[Int, ?]]",
           SerializableTests.serializable(Traverse[ColumnRef[Int, ?]]))

  checkAll("UsingColumn[Int, Int] with Option",
           TraverseTests[UsingColumn[Int, ?]].traverse[Int, Int, Int, Set[Int], Option, Option])
  checkAll("Traverse[UsingColumn[Int, ?]]",
           SerializableTests.serializable(Traverse[UsingColumn[Int, ?]]))

  checkAll("Query[Int, ?]", FunctorTests[Query[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Query[Int, ?]]", SerializableTests.serializable(Functor[Query[Int, ?]]))

  checkAll("With[Int, ?]", FunctorTests[With[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[With[Int, ?]]", SerializableTests.serializable(Functor[With[Int, ?]]))

  checkAll("QueryNoWith[Int, ?]", FunctorTests[QueryNoWith[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[QueryNoWith[Int, ?]]",
           SerializableTests.serializable(Functor[QueryNoWith[Int, ?]]))

  checkAll("OrderBy[Int, ?]", FunctorTests[OrderBy[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[OrderBy[Int, ?]]", SerializableTests.serializable(Functor[OrderBy[Int, ?]]))

  checkAll("QueryTerm[Int, ?]", FunctorTests[QueryTerm[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[QueryTerm[Int, ?]]", SerializableTests.serializable(Functor[QueryTerm[Int, ?]]))

  checkAll("SetOperation[Int, ?]", FunctorTests[SetOperation[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[SetOperation[Int, ?]]",
           SerializableTests.serializable(Functor[SetOperation[Int, ?]]))

  checkAll("QueryPrimary[Int, ?]", FunctorTests[QueryPrimary[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[QueryPrimary[Int, ?]]",
           SerializableTests.serializable(Functor[QueryPrimary[Int, ?]]))

  checkAll("QueryPrimaryTable[Int, ?]",
           FunctorTests[QueryPrimaryTable[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[QueryPrimaryTable[Int, ?]]",
           SerializableTests.serializable(Functor[QueryPrimaryTable[Int, ?]]))

  checkAll("InlineTable[Int, ?]", FunctorTests[InlineTable[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[InlineTable[Int, ?]]",
           SerializableTests.serializable(Functor[InlineTable[Int, ?]]))

  checkAll("SubQuery[Int, ?]", FunctorTests[SubQuery[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[SubQuery[Int, ?]]", SerializableTests.serializable(Functor[SubQuery[Int, ?]]))

  checkAll("SortItem[Int, ?]", FunctorTests[SortItem[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[SortItem[Int, ?]]", SerializableTests.serializable(Functor[SortItem[Int, ?]]))

  checkAll("QuerySpecification[Int, ?]",
           FunctorTests[QuerySpecification[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[QuerySpecification[Int, ?]]",
           SerializableTests.serializable(Functor[QuerySpecification[Int, ?]]))

  checkAll("GroupBy[Int, ?]", FunctorTests[GroupBy[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[GroupBy[Int, ?]]", SerializableTests.serializable(Functor[GroupBy[Int, ?]]))

  checkAll("GroupingElement[Int, ?]",
           FunctorTests[GroupingElement[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[GroupingElement[Int, ?]]",
           SerializableTests.serializable(Functor[GroupingElement[Int, ?]]))

  checkAll("SingleGroupingSet[Int, ?]",
           FunctorTests[SingleGroupingSet[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[SingleGroupingSet[Int, ?]]",
           SerializableTests.serializable(Functor[SingleGroupingSet[Int, ?]]))

  checkAll("Rollup[Int, ?]", FunctorTests[Rollup[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Rollup[Int, ?]]", SerializableTests.serializable(Functor[Rollup[Int, ?]]))

  checkAll("Cube[Int, ?]", FunctorTests[Cube[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Cube[Int, ?]]", SerializableTests.serializable(Functor[Cube[Int, ?]]))

  checkAll("MultipleGroupingSets[Int, ?]",
           FunctorTests[MultipleGroupingSets[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[MultipleGroupingSets[Int, ?]]",
           SerializableTests.serializable(Functor[MultipleGroupingSets[Int, ?]]))

  checkAll("GroupingSet[Int, ?]", FunctorTests[GroupingSet[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[GroupingSet[Int, ?]]",
           SerializableTests.serializable(Functor[GroupingSet[Int, ?]]))

  checkAll("NamedQuery[Int, ?]", FunctorTests[NamedQuery[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[NamedQuery[Int, ?]]",
           SerializableTests.serializable(Functor[NamedQuery[Int, ?]]))

  checkAll("SelectItem[Int, ?]", FunctorTests[SelectItem[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[SelectItem[Int, ?]]",
           SerializableTests.serializable(Functor[SelectItem[Int, ?]]))

  checkAll("SelectSingle[Int, ?]", FunctorTests[SelectSingle[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[SelectSingle[Int, ?]]",
           SerializableTests.serializable(Functor[SelectSingle[Int, ?]]))

  checkAll("SelectAll[Int, ?]", FunctorTests[SelectAll[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[SelectAll[Int, ?]]", SerializableTests.serializable(Functor[SelectAll[Int, ?]]))

  checkAll("Relation[Int, ?]", FunctorTests[Relation[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Relation[Int, ?]]", SerializableTests.serializable(Functor[Relation[Int, ?]]))

  checkAll("JoinRelation[Int, ?]", FunctorTests[JoinRelation[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[JoinRelation[Int, ?]]",
           SerializableTests.serializable(Functor[JoinRelation[Int, ?]]))

  checkAll("JoinCriteria[Int, ?]", FunctorTests[JoinCriteria[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[JoinCriteria[Int, ?]]",
           SerializableTests.serializable(Functor[JoinCriteria[Int, ?]]))

  checkAll("JoinOn[Int, ?]", FunctorTests[JoinOn[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[JoinOn[Int, ?]]", SerializableTests.serializable(Functor[JoinOn[Int, ?]]))

  checkAll("JoinUsing[Int, ?]", FunctorTests[JoinUsing[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[JoinUsing[Int, ?]]", SerializableTests.serializable(Functor[JoinUsing[Int, ?]]))

  checkAll("SampledRelation[Int, ?]",
           FunctorTests[SampledRelation[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[SampledRelation[Int, ?]]",
           SerializableTests.serializable(Functor[SampledRelation[Int, ?]]))

  checkAll("TableSample[Int, ?]", FunctorTests[TableSample[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[TableSample[Int, ?]]",
           SerializableTests.serializable(Functor[TableSample[Int, ?]]))

  checkAll("AliasedRelation[Int, ?]",
           FunctorTests[AliasedRelation[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[AliasedRelation[Int, ?]]",
           SerializableTests.serializable(Functor[AliasedRelation[Int, ?]]))

  checkAll("RelationPrimary[Int, ?]",
           FunctorTests[RelationPrimary[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[RelationPrimary[Int, ?]]",
           SerializableTests.serializable(Functor[RelationPrimary[Int, ?]]))

  checkAll("TableName[Int, Int] with Option",
           TraverseTests[TableName[Int, ?]].traverse[Int, Int, Int, Set[Int], Option, Option])
  checkAll("Traverse[TableName[Int, ?]]",
           SerializableTests.serializable(Traverse[TableName[Int, ?]]))

  checkAll("SubQueryRelation[Int, ?]",
           FunctorTests[SubQueryRelation[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[SubQueryRelation[Int, ?]]",
           SerializableTests.serializable(Functor[SubQueryRelation[Int, ?]]))

  checkAll("LateralRelation[Int, ?]",
           FunctorTests[LateralRelation[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[LateralRelation[Int, ?]]",
           SerializableTests.serializable(Functor[LateralRelation[Int, ?]]))

  checkAll("ParenthesizedRelation[Int, ?]",
           FunctorTests[ParenthesizedRelation[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[ParenthesizedRelation[Int, ?]]",
           SerializableTests.serializable(Functor[ParenthesizedRelation[Int, ?]]))

  checkAll("Expression[Int, ?]", FunctorTests[Expression[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Expression[Int, ?]]",
           SerializableTests.serializable(Functor[Expression[Int, ?]]))

  checkAll("LiteralExpr[Int, ?]", FunctorTests[LiteralExpr[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[LiteralExpr[Int, ?]]",
           SerializableTests.serializable(Functor[LiteralExpr[Int, ?]]))

  checkAll("ColumnExpr[Int, Int] with Option",
           TraverseTests[ColumnExpr[Int, ?]].traverse[Int, Int, Int, Set[Int], Option, Option])
  checkAll("Traverse[ColumnExpr[Int, ?]]",
           SerializableTests.serializable(Traverse[ColumnExpr[Int, ?]]))

  checkAll("SubQueryExpr[Int, ?]", FunctorTests[SubQueryExpr[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[SubQueryExpr[Int, ?]]",
           SerializableTests.serializable(Functor[SubQueryExpr[Int, ?]]))

  checkAll("BooleanExpr[Int, ?]", FunctorTests[BooleanExpr[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[BooleanExpr[Int, ?]]",
           SerializableTests.serializable(Functor[BooleanExpr[Int, ?]]))

  checkAll("ComparisonExpr[Int, ?]", FunctorTests[ComparisonExpr[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[ComparisonExpr[Int, ?]]",
           SerializableTests.serializable(Functor[ComparisonExpr[Int, ?]]))

  checkAll("DereferenceExpr[Int, ?]",
           FunctorTests[DereferenceExpr[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[DereferenceExpr[Int, ?]]",
           SerializableTests.serializable(Functor[DereferenceExpr[Int, ?]]))

  checkAll("FunctionCall[Int, ?]", FunctorTests[FunctionCall[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[FunctionCall[Int, ?]]",
           SerializableTests.serializable(Functor[FunctionCall[Int, ?]]))

  checkAll("FunctionFilter[Int, ?]", FunctorTests[FunctionFilter[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[FunctionFilter[Int, ?]]",
           SerializableTests.serializable(Functor[FunctionFilter[Int, ?]]))

  checkAll("FunctionOver[Int, ?]", FunctorTests[FunctionOver[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[FunctionOver[Int, ?]]",
           SerializableTests.serializable(Functor[FunctionOver[Int, ?]]))

  checkAll("WindowFrame[Int, ?]", FunctorTests[WindowFrame[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[WindowFrame[Int, ?]]",
           SerializableTests.serializable(Functor[WindowFrame[Int, ?]]))

  checkAll("FrameBound[Int, ?]", FunctorTests[FrameBound[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[FrameBound[Int, ?]]",
           SerializableTests.serializable(Functor[FrameBound[Int, ?]]))

  checkAll("UnboundedFrame[Int, ?]", FunctorTests[UnboundedFrame[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[UnboundedFrame[Int, ?]]",
           SerializableTests.serializable(Functor[UnboundedFrame[Int, ?]]))

  checkAll("CurrentRowBound[Int, ?]",
           FunctorTests[CurrentRowBound[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[CurrentRowBound[Int, ?]]",
           SerializableTests.serializable(Functor[CurrentRowBound[Int, ?]]))

  checkAll("BoundedFrame[Int, ?]", FunctorTests[BoundedFrame[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[BoundedFrame[Int, ?]]",
           SerializableTests.serializable(Functor[BoundedFrame[Int, ?]]))
}