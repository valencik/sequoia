package ca.valencik.sequoia

import cats.implicits._
import cats.{Functor, Traverse}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.Checkers
import org.typelevel.discipline.scalatest.FunSuiteDiscipline
import cats.laws.discipline.{FunctorTests, SerializableTests, TraverseTests}

import ca.valencik.sequoia.arbitrary._

class TableRefLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "TableRef[Int, Int] with Option",
    TraverseTests[TableRef[Int, ?]].traverse[Int, Int, Int, Set[Int], Option, Option]
  )
  checkAll("Traverse[TableRef[Int, ?]]", SerializableTests.serializable(Traverse[TableRef[Int, ?]]))
}

class ColumnRefLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "ColumnRef[Int, Int] with Option",
    TraverseTests[ColumnRef[Int, ?]].traverse[Int, Int, Int, Set[Int], Option, Option]
  )
  checkAll(
    "Traverse[ColumnRef[Int, ?]]",
    SerializableTests.serializable(Traverse[ColumnRef[Int, ?]])
  )
}

class UsingColumnLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "UsingColumn[Int, Int] with Option",
    TraverseTests[UsingColumn[Int, ?]].traverse[Int, Int, Int, Set[Int], Option, Option]
  )
  checkAll(
    "Traverse[UsingColumn[Int, ?]]",
    SerializableTests.serializable(Traverse[UsingColumn[Int, ?]])
  )

}

class QueryLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("Query[Int, ?]", FunctorTests[Query[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Query[Int, ?]]", SerializableTests.serializable(Functor[Query[Int, ?]]))

  checkAll("With[Int, ?]", FunctorTests[With[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[With[Int, ?]]", SerializableTests.serializable(Functor[With[Int, ?]]))

  checkAll("QueryNoWith[Int, ?]", FunctorTests[QueryNoWith[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[QueryNoWith[Int, ?]]",
    SerializableTests.serializable(Functor[QueryNoWith[Int, ?]])
  )
}

class OrderByLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("OrderBy[Int, ?]", FunctorTests[OrderBy[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[OrderBy[Int, ?]]", SerializableTests.serializable(Functor[OrderBy[Int, ?]]))
}

class QueryTermLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("QueryTerm[Int, ?]", FunctorTests[QueryTerm[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[QueryTerm[Int, ?]]", SerializableTests.serializable(Functor[QueryTerm[Int, ?]]))
}

class SetOperationLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("SetOperation[Int, ?]", FunctorTests[SetOperation[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[SetOperation[Int, ?]]",
    SerializableTests.serializable(Functor[SetOperation[Int, ?]])
  )
}

class QueryPrimaryLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("QueryPrimary[Int, ?]", FunctorTests[QueryPrimary[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[QueryPrimary[Int, ?]]",
    SerializableTests.serializable(Functor[QueryPrimary[Int, ?]])
  )
}

class QueryPrimaryTableLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "QueryPrimaryTable[Int, ?]",
    FunctorTests[QueryPrimaryTable[Int, ?]].functor[Int, Int, String]
  )
  checkAll(
    "Functor[QueryPrimaryTable[Int, ?]]",
    SerializableTests.serializable(Functor[QueryPrimaryTable[Int, ?]])
  )
}

class InlineTableLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("InlineTable[Int, ?]", FunctorTests[InlineTable[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[InlineTable[Int, ?]]",
    SerializableTests.serializable(Functor[InlineTable[Int, ?]])
  )
}

class SubQueryLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("SubQuery[Int, ?]", FunctorTests[SubQuery[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[SubQuery[Int, ?]]", SerializableTests.serializable(Functor[SubQuery[Int, ?]]))
}

class SortItemLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("SortItem[Int, ?]", FunctorTests[SortItem[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[SortItem[Int, ?]]", SerializableTests.serializable(Functor[SortItem[Int, ?]]))
}

class QuerySpecificationLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "QuerySpecification[Int, ?]",
    FunctorTests[QuerySpecification[Int, ?]].functor[Int, Int, String]
  )
  checkAll(
    "Functor[QuerySpecification[Int, ?]]",
    SerializableTests.serializable(Functor[QuerySpecification[Int, ?]])
  )
}

class GroupByLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("GroupBy[Int, ?]", FunctorTests[GroupBy[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[GroupBy[Int, ?]]", SerializableTests.serializable(Functor[GroupBy[Int, ?]]))
}

class GroupingElementLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "GroupingElement[Int, ?]",
    FunctorTests[GroupingElement[Int, ?]].functor[Int, Int, String]
  )
  checkAll(
    "Functor[GroupingElement[Int, ?]]",
    SerializableTests.serializable(Functor[GroupingElement[Int, ?]])
  )
}

class SingleGroupingSetLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "SingleGroupingSet[Int, ?]",
    FunctorTests[SingleGroupingSet[Int, ?]].functor[Int, Int, String]
  )
  checkAll(
    "Functor[SingleGroupingSet[Int, ?]]",
    SerializableTests.serializable(Functor[SingleGroupingSet[Int, ?]])
  )
}

class RollupLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("Rollup[Int, ?]", FunctorTests[Rollup[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Rollup[Int, ?]]", SerializableTests.serializable(Functor[Rollup[Int, ?]]))
}

class CubeLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("Cube[Int, ?]", FunctorTests[Cube[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Cube[Int, ?]]", SerializableTests.serializable(Functor[Cube[Int, ?]]))
}

class MultipleGroupingSetsLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "MultipleGroupingSets[Int, ?]",
    FunctorTests[MultipleGroupingSets[Int, ?]].functor[Int, Int, String]
  )
  checkAll(
    "Functor[MultipleGroupingSets[Int, ?]]",
    SerializableTests.serializable(Functor[MultipleGroupingSets[Int, ?]])
  )
}

class GroupingSetLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("GroupingSet[Int, ?]", FunctorTests[GroupingSet[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[GroupingSet[Int, ?]]",
    SerializableTests.serializable(Functor[GroupingSet[Int, ?]])
  )
}

class NamedQueryLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("NamedQuery[Int, ?]", FunctorTests[NamedQuery[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[NamedQuery[Int, ?]]",
    SerializableTests.serializable(Functor[NamedQuery[Int, ?]])
  )
}

class SelectItemLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("SelectItem[Int, ?]", FunctorTests[SelectItem[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[SelectItem[Int, ?]]",
    SerializableTests.serializable(Functor[SelectItem[Int, ?]])
  )
}

class SelectSingleLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("SelectSingle[Int, ?]", FunctorTests[SelectSingle[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[SelectSingle[Int, ?]]",
    SerializableTests.serializable(Functor[SelectSingle[Int, ?]])
  )
}

class SelectAllLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("SelectAll[Int, ?]", FunctorTests[SelectAll[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[SelectAll[Int, ?]]", SerializableTests.serializable(Functor[SelectAll[Int, ?]]))
}

class RelationLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("Relation[Int, ?]", FunctorTests[Relation[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Relation[Int, ?]]", SerializableTests.serializable(Functor[Relation[Int, ?]]))
}

class JoinRelationLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("JoinRelation[Int, ?]", FunctorTests[JoinRelation[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[JoinRelation[Int, ?]]",
    SerializableTests.serializable(Functor[JoinRelation[Int, ?]])
  )
}

class JoinCriteriaLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("JoinCriteria[Int, ?]", FunctorTests[JoinCriteria[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[JoinCriteria[Int, ?]]",
    SerializableTests.serializable(Functor[JoinCriteria[Int, ?]])
  )
}

class JoinOnLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("JoinOn[Int, ?]", FunctorTests[JoinOn[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[JoinOn[Int, ?]]", SerializableTests.serializable(Functor[JoinOn[Int, ?]]))
}

class JoinUsingLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("JoinUsing[Int, ?]", FunctorTests[JoinUsing[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[JoinUsing[Int, ?]]", SerializableTests.serializable(Functor[JoinUsing[Int, ?]]))
}

class SampledRelationLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "SampledRelation[Int, ?]",
    FunctorTests[SampledRelation[Int, ?]].functor[Int, Int, String]
  )
  checkAll(
    "Functor[SampledRelation[Int, ?]]",
    SerializableTests.serializable(Functor[SampledRelation[Int, ?]])
  )
}

class TableSampleLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("TableSample[Int, ?]", FunctorTests[TableSample[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[TableSample[Int, ?]]",
    SerializableTests.serializable(Functor[TableSample[Int, ?]])
  )
}

class AliasedRelationLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "AliasedRelation[Int, ?]",
    FunctorTests[AliasedRelation[Int, ?]].functor[Int, Int, String]
  )
  checkAll(
    "Functor[AliasedRelation[Int, ?]]",
    SerializableTests.serializable(Functor[AliasedRelation[Int, ?]])
  )
}

class RelationPrimaryLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "RelationPrimary[Int, ?]",
    FunctorTests[RelationPrimary[Int, ?]].functor[Int, Int, String]
  )
  checkAll(
    "Functor[RelationPrimary[Int, ?]]",
    SerializableTests.serializable(Functor[RelationPrimary[Int, ?]])
  )
}

class TableNameLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "TableName[Int, Int] with Option",
    TraverseTests[TableName[Int, ?]].traverse[Int, Int, Int, Set[Int], Option, Option]
  )
  checkAll(
    "Traverse[TableName[Int, ?]]",
    SerializableTests.serializable(Traverse[TableName[Int, ?]])
  )
}

class UnnestLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("Unnest[Int, ?]", FunctorTests[Unnest[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Unnest[Int, ?]]", SerializableTests.serializable(Functor[Unnest[Int, ?]]))
}

class SubQueryRelationLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "SubQueryRelation[Int, ?]",
    FunctorTests[SubQueryRelation[Int, ?]].functor[Int, Int, String]
  )
  checkAll(
    "Functor[SubQueryRelation[Int, ?]]",
    SerializableTests.serializable(Functor[SubQueryRelation[Int, ?]])
  )
}

class LateralRelationLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "LateralRelation[Int, ?]",
    FunctorTests[LateralRelation[Int, ?]].functor[Int, Int, String]
  )
  checkAll(
    "Functor[LateralRelation[Int, ?]]",
    SerializableTests.serializable(Functor[LateralRelation[Int, ?]])
  )
}

class ParenthesizedRelationLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "ParenthesizedRelation[Int, ?]",
    FunctorTests[ParenthesizedRelation[Int, ?]].functor[Int, Int, String]
  )
  checkAll(
    "Functor[ParenthesizedRelation[Int, ?]]",
    SerializableTests.serializable(Functor[ParenthesizedRelation[Int, ?]])
  )
}

class ExpressionLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("Expression[Int, ?]", FunctorTests[Expression[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[Expression[Int, ?]]",
    SerializableTests.serializable(Functor[Expression[Int, ?]])
  )
}

class LogicalBinaryLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("LogicalBinary[Int, ?]", FunctorTests[LogicalBinary[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[LogicalBinary[Int, ?]]",
    SerializableTests.serializable(Functor[LogicalBinary[Int, ?]])
  )
}

class PredicateLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("Predicate[Int, ?]", FunctorTests[Predicate[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Predicate[Int, ?]]", SerializableTests.serializable(Functor[Predicate[Int, ?]]))
}

class NotPredicateLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("NotPredicate[Int, ?]", FunctorTests[NotPredicate[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[NotPredicate[Int, ?]]",
    SerializableTests.serializable(Functor[NotPredicate[Int, ?]])
  )
}

class ComparisonExprLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("ComparisonExpr[Int, ?]", FunctorTests[ComparisonExpr[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[ComparisonExpr[Int, ?]]",
    SerializableTests.serializable(Functor[ComparisonExpr[Int, ?]])
  )
}

class QuantifiedComparisonLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "QuantifiedComparison[Int, ?]",
    FunctorTests[QuantifiedComparison[Int, ?]].functor[Int, Int, String]
  )
  checkAll(
    "Functor[QuantifiedComparison[Int, ?]]",
    SerializableTests.serializable(Functor[QuantifiedComparison[Int, ?]])
  )
}

class BetweenLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("Between[Int, ?]", FunctorTests[Between[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Between[Int, ?]]", SerializableTests.serializable(Functor[Between[Int, ?]]))
}

class InListLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("InList[Int, ?]", FunctorTests[InList[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[InList[Int, ?]]", SerializableTests.serializable(Functor[InList[Int, ?]]))
}

class InSubQueryLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("InSubQuery[Int, ?]", FunctorTests[InSubQuery[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[InSubQuery[Int, ?]]",
    SerializableTests.serializable(Functor[InSubQuery[Int, ?]])
  )
}

class LikeLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("Like[Int, ?]", FunctorTests[Like[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Like[Int, ?]]", SerializableTests.serializable(Functor[Like[Int, ?]]))
}

class NullPredicateLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("NullPredicate[Int, ?]", FunctorTests[NullPredicate[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[NullPredicate[Int, ?]]",
    SerializableTests.serializable(Functor[NullPredicate[Int, ?]])
  )
}

class DistinctFromLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("DistinctFrom[Int, ?]", FunctorTests[DistinctFrom[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[DistinctFrom[Int, ?]]",
    SerializableTests.serializable(Functor[DistinctFrom[Int, ?]])
  )
}

class ValueExpressionLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "ValueExpression[Int, ?]",
    FunctorTests[ValueExpression[Int, ?]].functor[Int, Int, String]
  )
  checkAll(
    "Functor[ValueExpression[Int, ?]]",
    SerializableTests.serializable(Functor[ValueExpression[Int, ?]])
  )
}

class ArithmeticUnaryLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "ArithmeticUnary[Int, ?]",
    FunctorTests[ArithmeticUnary[Int, ?]].functor[Int, Int, String]
  )
  checkAll(
    "Functor[ArithmeticUnary[Int, ?]]",
    SerializableTests.serializable(Functor[ArithmeticUnary[Int, ?]])
  )
}

class ArithmeticBinaryLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "ArithmeticBinary[Int, ?]",
    FunctorTests[ArithmeticBinary[Int, ?]].functor[Int, Int, String]
  )
  checkAll(
    "Functor[ArithmeticBinary[Int, ?]]",
    SerializableTests.serializable(Functor[ArithmeticBinary[Int, ?]])
  )
}

class PrimaryExpressionLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "PrimaryExpression[Int, ?]",
    FunctorTests[PrimaryExpression[Int, ?]].functor[Int, Int, String]
  )
  checkAll(
    "Functor[PrimaryExpression[Int, ?]]",
    SerializableTests.serializable(Functor[PrimaryExpression[Int, ?]])
  )
}

class LiteralExprLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("LiteralExpr[Int, ?]", FunctorTests[LiteralExpr[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[LiteralExpr[Int, ?]]",
    SerializableTests.serializable(Functor[LiteralExpr[Int, ?]])
  )
}

class ColumnExprLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "ColumnExpr[Int, Int] with Option",
    TraverseTests[ColumnExpr[Int, ?]].traverse[Int, Int, Int, Set[Int], Option, Option]
  )
  checkAll(
    "Traverse[ColumnExpr[Int, ?]]",
    SerializableTests.serializable(Traverse[ColumnExpr[Int, ?]])
  )
}

class SubQueryExprLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("SubQueryExpr[Int, ?]", FunctorTests[SubQueryExpr[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[SubQueryExpr[Int, ?]]",
    SerializableTests.serializable(Functor[SubQueryExpr[Int, ?]])
  )
}

class ExistsExprLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("ExistsExpr[Int, ?]", FunctorTests[ExistsExpr[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[ExistsExpr[Int, ?]]",
    SerializableTests.serializable(Functor[ExistsExpr[Int, ?]])
  )
}

class SimpleCaseLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("SimpleCase[Int, ?]", FunctorTests[SimpleCase[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[SimpleCase[Int, ?]]",
    SerializableTests.serializable(Functor[SimpleCase[Int, ?]])
  )
}

class SearchedCaseLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("SearchedCase[Int, ?]", FunctorTests[SearchedCase[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[SearchedCase[Int, ?]]",
    SerializableTests.serializable(Functor[SearchedCase[Int, ?]])
  )
}

class WhenClauseLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("WhenClause[Int, ?]", FunctorTests[WhenClause[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[WhenClause[Int, ?]]",
    SerializableTests.serializable(Functor[WhenClause[Int, ?]])
  )
}

class CastLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("Cast[Int, ?]", FunctorTests[Cast[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Cast[Int, ?]]", SerializableTests.serializable(Functor[Cast[Int, ?]]))
}

class SubscriptLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("Subscript[Int, ?]", FunctorTests[Subscript[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Subscript[Int, ?]]", SerializableTests.serializable(Functor[Subscript[Int, ?]]))
}

class DereferenceExprLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "DereferenceExpr[Int, ?]",
    FunctorTests[DereferenceExpr[Int, ?]].functor[Int, Int, String]
  )
  checkAll(
    "Functor[DereferenceExpr[Int, ?]]",
    SerializableTests.serializable(Functor[DereferenceExpr[Int, ?]])
  )
}

class RowLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("Row[Int, ?]", FunctorTests[Row[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Row[Int, ?]]", SerializableTests.serializable(Functor[Row[Int, ?]]))
}

class FunctionCallLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("FunctionCall[Int, ?]", FunctorTests[FunctionCall[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[FunctionCall[Int, ?]]",
    SerializableTests.serializable(Functor[FunctionCall[Int, ?]])
  )
}

class FunctionFilterLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("FunctionFilter[Int, ?]", FunctorTests[FunctionFilter[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[FunctionFilter[Int, ?]]",
    SerializableTests.serializable(Functor[FunctionFilter[Int, ?]])
  )
}

class FunctionOverLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("FunctionOver[Int, ?]", FunctorTests[FunctionOver[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[FunctionOver[Int, ?]]",
    SerializableTests.serializable(Functor[FunctionOver[Int, ?]])
  )
}

class WindowFrameLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("WindowFrame[Int, ?]", FunctorTests[WindowFrame[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[WindowFrame[Int, ?]]",
    SerializableTests.serializable(Functor[WindowFrame[Int, ?]])
  )
}

class FrameBoundLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("FrameBound[Int, ?]", FunctorTests[FrameBound[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[FrameBound[Int, ?]]",
    SerializableTests.serializable(Functor[FrameBound[Int, ?]])
  )
}

class UnboundedFrameLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("UnboundedFrame[Int, ?]", FunctorTests[UnboundedFrame[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[UnboundedFrame[Int, ?]]",
    SerializableTests.serializable(Functor[UnboundedFrame[Int, ?]])
  )
}

class CurrentRowBoundLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "CurrentRowBound[Int, ?]",
    FunctorTests[CurrentRowBound[Int, ?]].functor[Int, Int, String]
  )
  checkAll(
    "Functor[CurrentRowBound[Int, ?]]",
    SerializableTests.serializable(Functor[CurrentRowBound[Int, ?]])
  )
}

class BoundedFrameLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("BoundedFrame[Int, ?]", FunctorTests[BoundedFrame[Int, ?]].functor[Int, Int, String])
  checkAll(
    "Functor[BoundedFrame[Int, ?]]",
    SerializableTests.serializable(Functor[BoundedFrame[Int, ?]])
  )
}

class IntervalLiteralLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "IntervalLiteral[Int, ?]",
    FunctorTests[IntervalLiteral[Int, ?]].functor[Int, Int, String]
  )
  checkAll(
    "Functor[IntervalLiteral[Int, ?]]",
    SerializableTests.serializable(Functor[IntervalLiteral[Int, ?]])
  )
}

class ExtractLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll("Extract[Int, ?]", FunctorTests[Extract[Int, ?]].functor[Int, Int, String])
  checkAll("Functor[Extract[Int, ?]]", SerializableTests.serializable(Functor[Extract[Int, ?]]))
}

class SpecialDateTimeFuncLawTests extends AnyFunSuite with FunSuiteDiscipline with Checkers {
  checkAll(
    "SpecialDateTimeFunc[Int, ?]",
    FunctorTests[SpecialDateTimeFunc[Int, ?]].functor[Int, Int, String]
  )
  checkAll(
    "Functor[SpecialDateTimeFunc[Int, ?]]",
    SerializableTests.serializable(Functor[SpecialDateTimeFunc[Int, ?]])
  )
}
