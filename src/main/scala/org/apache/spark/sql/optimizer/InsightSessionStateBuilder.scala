// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.optimizer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.analysis.DetectAmbiguousSelfJoin
import org.apache.spark.sql.execution.command.CommandCheck
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2.TableCapabilityCheck
import org.apache.spark.sql.hive._
import org.apache.spark.sql.internal.SessionState

/**
 * @see HiveSessionStateBuilder
 *
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-07-30.
 */
class InsightSessionStateBuilder(session: SparkSession, parentState: Option[SessionState] = None)
  extends HiveSessionStateBuilder(session, parentState) {

  /**
   * A logical query plan `Analyzer` with rules specific for Insight.
   *
   * Added Rules:
   *   ExternalRelationRule
   *
   * Deleted Rules:
   */
  override def analyzer: Analyzer = new Analyzer(catalogManager, conf) {

    private val v1SessionCatalog: SessionCatalog = catalogManager.v1SessionCatalog

    override lazy val batches: Seq[Batch] = Seq(
      Batch("Hints", fixedPoint,
        new ResolveHints.ResolveJoinStrategyHints(conf),
        new ResolveHints.ResolveCoalesceHints(conf),
        ExternalRelationRule(session),
        CollectValueRule,
        DataQualityRule(session)
      ),
      Batch("Simple Sanity Check", Once,
        LookupFunctions),
      Batch("Substitution", fixedPoint,
        CTESubstitution,
        WindowsSubstitution,
        EliminateUnions,
        new SubstituteUnresolvedOrdinals(conf)),
      Batch("Resolution", fixedPoint,
        ResolveTableValuedFunctions ::
          ResolveNamespace(catalogManager) ::
          new ResolveCatalogs(catalogManager) ::
          ResolveInsertInto ::
          ResolveRelations ::
          ResolveTables ::
          ResolveReferences ::
          ResolveCreateNamedStruct ::
          ResolveDeserializer ::
          ResolveNewInstance ::
          ResolveUpCast ::
          ResolveGroupingAnalytics ::
          ResolvePivot ::
          ResolveOrdinalInOrderByAndGroupBy ::
          ResolveAggAliasInGroupBy ::
          ResolveMissingReferences ::
          ExtractGenerator ::
          ResolveGenerate ::
          ResolveFunctions ::
          ResolveAliases ::
          ResolveSubquery ::
          ResolveSubqueryColumnAliases ::
          ResolveWindowOrder ::
          ResolveWindowFrame ::
          ResolveNaturalAndUsingJoin ::
          ResolveOutputRelation ::
          ExtractWindowExpressions ::
          GlobalAggregates ::
          ResolveAggregateFunctions ::
          TimeWindowing ::
          ResolveInlineTables(conf) ::
          ResolveHigherOrderFunctions(v1SessionCatalog) ::
          ResolveLambdaVariables(conf) ::
          ResolveTimeZone(conf) ::
          ResolveRandomSeed ::
          ResolveBinaryArithmetic ::
          TypeCoercion.typeCoercionRules(conf) ++
            extendedResolutionRules: _*),
      Batch("Post-Hoc Resolution", Once, postHocResolutionRules: _*),
      Batch("Normalize Alter Table", Once, ResolveAlterTableChanges),
      Batch("Remove Unresolved Hints", Once,
        new ResolveHints.RemoveAllHints(conf)),
      Batch("Nondeterministic", Once,
        PullOutNondeterministic),
      Batch("UDF", Once,
        HandleNullInputsForUDF),
      Batch("UpdateNullability", Once,
        UpdateAttributeNullability),
      Batch("Subquery", Once,
        UpdateOuterReferences),
      Batch("Cleanup", fixedPoint,
        CleanupAliases)
    )

    override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
      new ResolveHiveSerdeTable(session) +:
        new FindDataSourceTable(session) +:
        new ResolveSQLOnFile(session) +:
        new FallBackFileSourceV2(session) +:
        new ResolveSessionCatalog(
          catalogManager, conf, catalog.isTempView, catalog.isTempFunction) +:
        customResolutionRules

    override val extendedCheckRules: Seq[LogicalPlan => Unit] =
      PreWriteCheck +:
        PreReadCheck +:
        TableCapabilityCheck +:
        CommandCheck(conf) +:
        customCheckRules

    override val postHocResolutionRules: Seq[Rule[LogicalPlan]] =
      new DetectAmbiguousSelfJoin(conf) +:
        new DetermineTableStats(session) +:
        RelationConversions(conf, catalog) +:
        PreprocessTableCreation(session) +:
        PreprocessTableInsertion(conf) +:
        DataSourceAnalysis(conf) +:
        HiveAnalysis +:
        customPostHocResolutionRules
  }

  override protected def newBuilder: NewBuilder = new InsightSessionStateBuilder(_, _)
}
