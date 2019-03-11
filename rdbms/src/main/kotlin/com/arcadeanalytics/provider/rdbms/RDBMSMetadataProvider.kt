/*-
 * #%L
 * Arcade Data
 * %%
 * Copyright (C) 2018 - 2019 ArcadeAnalytics
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.arcadeanalytics.provider.rdbms

import com.arcadeanalytics.provider.*
import com.arcadeanalytics.provider.rdbms.context.Statistics
import com.arcadeanalytics.provider.rdbms.dbengine.DBQueryEngine
import com.arcadeanalytics.provider.rdbms.factory.DataTypeHandlerFactory
import com.arcadeanalytics.provider.rdbms.factory.NameResolverFactory
import com.arcadeanalytics.provider.rdbms.factory.StrategyFactory
import com.arcadeanalytics.provider.rdbms.mapper.rdbms.ER2GraphMapper
import com.arcadeanalytics.provider.rdbms.persistence.util.QueryResult
import com.arcadeanalytics.provider.rdbms.persistence.util.RelationshipQueryResult
import com.arcadeanalytics.provider.rdbms.strategy.rdbms.AbstractDBMSModelBuildingStrategy
import java.sql.ResultSet

class RDBMSMetadataProvider : DataSourceMetadataProvider {

    override fun supportedDataSourceTypes(): Set<String> = setOf(
            "RDBMS_POSTGRESQL",
            "RDBMS_MYSQL",
            "RDBMS_MSSQLSERVER",
            "RDBMS_HSQL",
            "RDBMS_ORACLE",
            "RDBMS_DATA_WORLD"
    )

    override fun fetchMetadata(dataSource: DataSourceInfo): DataSourceMetadata {

        val mapper: ER2GraphMapper = getMapper(dataSource)

        val dbQueryEngine: DBQueryEngine = DBQueryEngine(dataSource, 300)

        val graphModel = mapper.graphModel

        val nodesClasses = graphModel.verticesType
                .map {
                    val props = it.allProperties
                            .map { prop -> prop.name to TypeProperty(prop.name, prop.orientdbType) }
                            .toMap()

                    var cardinality: Long = 0
                    if (dataSource.aggregationEnabled) {

                        if (!it.isFromJoinTable) {
                            mapper.vertexType2EVClassMappers.get(it)?.get(0)?.entity?.name?.let { tableName ->
                                val queryResult: QueryResult = dbQueryEngine.countTableRecords(tableName)
                                val countResult: ResultSet = queryResult.result
                                if (countResult.next()) {
                                    cardinality = countResult.getLong(1)
                                }
                                queryResult.close()
                            }
                        }
                    } else {
                        mapper.vertexType2EVClassMappers.get(it)?.get(0)?.entity?.name.let { tableName ->
                            val queryResult: QueryResult = dbQueryEngine.countTableRecords(tableName)
                            val countResult: ResultSet = queryResult.result
                            if (countResult.next()) {
                                cardinality = countResult.getLong(1)
                            }
                            queryResult.close()
                        }
                    }

                    TypeClass(it.name, cardinality, props)
                }.map {
                    it.name to it
                }.toMap()


        val edgesClasses: EdgesClasses = graphModel.edgesType
                .map { edgeType ->
                    val props = edgeType.allProperties
                            .map { prop -> prop.name to TypeProperty(prop.name, prop.orientdbType) }
                            .toMap()

                    val edgeTypeName: String = edgeType.name

                    var cardinality: Long = 0

                    if (dataSource.aggregationEnabled) {

                        if (edgeType.isAggregatorEdge) {
                            mapper.getJoinVertexTypeByAggregatorEdgeName(edgeTypeName)?.run {
                                val joinTable = mapper.getEntityByVertexType(this, 0) // join vertex has always 1-1 mapping with the join table, so I always get the first mapping
                                val queryResult: QueryResult = dbQueryEngine.countTableRecords(joinTable.name)
                                val countResult: ResultSet = queryResult.result
                                if (countResult.next()) {
                                    cardinality += countResult.getLong(1)
                                }
                                queryResult.close()
                            }
                        } else {
                            val mappedRelationships = mapper.edgeType2relationships.get(edgeType)
                            mappedRelationships?.forEach {
                                if (!it.foreignEntity.isAggregableJoinTable) {   // excluding relationships that are aggregated in aggregator edges, then all that have a join table as parent entity
                                    val queryResult: RelationshipQueryResult = dbQueryEngine.computeRelationshipCardinality(it, dataSource, edgeTypeName)
                                    val countResult: ResultSet = queryResult.result
                                    if (countResult.next()) {
                                        cardinality += countResult.getLong(1)
                                    }
                                    queryResult.close()
                                }
                            }
                        }

                    } else {
                        val mappedRelationships = mapper.edgeType2relationships.get(edgeType)
                        mappedRelationships?.forEach { rel ->
                            val queryResult: RelationshipQueryResult = dbQueryEngine.computeRelationshipCardinality(rel, dataSource, edgeTypeName)
                            val countResult: ResultSet = queryResult.result
                            if (countResult.next()) {
                                cardinality += countResult.getLong(1)
                            }
                            queryResult.close()
                        }
                    }
                    TypeClass(edgeType.name, cardinality, props)
                }.map {
                    it.name to it
                }.toMap()


        dbQueryEngine.close()
        return DataSourceMetadata(nodesClasses, edgesClasses)
    }

    private fun getMapper(dataSource: DataSourceInfo): ER2GraphMapper {

        val dbQueryEngine = DBQueryEngine(dataSource, 300)

        val statistics = Statistics()

        val aggregate = dataSource.aggregationEnabled

        val chosenStrategy = if (aggregate) "interactive-aggr" else "interactive"

        val dataTypeHandlerFactory = DataTypeHandlerFactory()
        val handler = dataTypeHandlerFactory.buildDataTypeHandler(dataSource.type)

        val nameResolverFactory = NameResolverFactory()
        val nameResolver = nameResolverFactory.buildNameResolver("original")

        val strategyFactory = StrategyFactory()
        val mapper = (strategyFactory.buildStrategy(chosenStrategy) as AbstractDBMSModelBuildingStrategy)
                .createSchemaMapper(dataSource,
                        null,
                        "basicDBMapper",
                        null,
                        nameResolver,
                        handler,
                        null,
                        null,
                        chosenStrategy,
                        dbQueryEngine,
                        statistics)

        return mapper as ER2GraphMapper

    }

}
