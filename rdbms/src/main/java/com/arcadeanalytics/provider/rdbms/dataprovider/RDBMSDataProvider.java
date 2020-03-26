package com.arcadeanalytics.provider.rdbms.dataprovider;

/*-
 * #%L
 * Arcade Connectors
 * %%
 * Copyright (C) 2018 - 2020 ArcadeData
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

import com.arcadeanalytics.provider.CytoData;
import com.arcadeanalytics.provider.DataSourceGraphDataProvider;
import com.arcadeanalytics.provider.DataSourceInfo;
import com.arcadeanalytics.provider.GraphData;
import com.arcadeanalytics.provider.rdbms.context.Statistics;
import com.arcadeanalytics.provider.rdbms.dbengine.DBQueryEngine;
import com.arcadeanalytics.provider.rdbms.exception.RDBMSProviderAggregationException;
import com.arcadeanalytics.provider.rdbms.exception.RDBMSProviderIOException;
import com.arcadeanalytics.provider.rdbms.exception.RDBMSProviderOperationNotAllowedException;
import com.arcadeanalytics.provider.rdbms.exception.RDBMSProviderRuntimeException;
import com.arcadeanalytics.provider.rdbms.factory.DataTypeHandlerFactory;
import com.arcadeanalytics.provider.rdbms.factory.NameResolverFactory;
import com.arcadeanalytics.provider.rdbms.factory.StrategyFactory;
import com.arcadeanalytics.provider.rdbms.mapper.rdbms.AggregatorEdge;
import com.arcadeanalytics.provider.rdbms.mapper.rdbms.ER2GraphMapper;
import com.arcadeanalytics.provider.rdbms.model.dbschema.Attribute;
import com.arcadeanalytics.provider.rdbms.model.dbschema.CanonicalRelationship;
import com.arcadeanalytics.provider.rdbms.model.dbschema.Entity;
import com.arcadeanalytics.provider.rdbms.model.dbschema.Relationship;
import com.arcadeanalytics.provider.rdbms.model.graphmodel.EdgeType;
import com.arcadeanalytics.provider.rdbms.model.graphmodel.VertexType;
import com.arcadeanalytics.provider.rdbms.nameresolver.NameResolver;
import com.arcadeanalytics.provider.rdbms.persistence.handler.DBMSDataTypeHandler;
import com.arcadeanalytics.provider.rdbms.persistence.util.DBSourceConnection;
import com.arcadeanalytics.provider.rdbms.persistence.util.QueryResult;
import com.arcadeanalytics.provider.rdbms.persistence.util.RelationshipQueryResult;
import com.arcadeanalytics.provider.rdbms.strategy.rdbms.AbstractDBMSModelBuildingStrategy;
import com.google.common.collect.Sets;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class RDBMSDataProvider implements DataSourceGraphDataProvider {

    private final Logger log = LoggerFactory.getLogger(RDBMSDataProvider.class);

    private final DataFetcher dataFetcher;
    private final Statistics statistics;

    private DBQueryEngine dbQueryEngine;
    private String datasourceId;

    public RDBMSDataProvider() {

        dataFetcher = new DataFetcher();

        statistics = new Statistics();

    }

    public void prepareMapperAndDataFetcher(final DataSourceInfo datasource) {


        datasourceId = Optional.ofNullable(datasource.getId()).orElse(-1L).toString();

        dbQueryEngine = new DBQueryEngine(datasource, 300);

        final Boolean aggregate = Optional.ofNullable(datasource.isAggregationEnabled()).orElse(false);
        String chosenStrategy = aggregate ? "interactive-aggr" : "interactive";


        ER2GraphMapper mapper;


        DataTypeHandlerFactory dataTypeHandlerFactory = new DataTypeHandlerFactory();
        DBMSDataTypeHandler handler = (DBMSDataTypeHandler) dataTypeHandlerFactory.buildDataTypeHandler(datasource.getType());

        NameResolverFactory nameResolverFactory = new NameResolverFactory();
        NameResolver nameResolver = nameResolverFactory.buildNameResolver("original");

        StrategyFactory strategyFactory = new StrategyFactory();
        try {
            mapper = ((AbstractDBMSModelBuildingStrategy) strategyFactory.buildStrategy(chosenStrategy))
                    .createSchemaMapper(datasource,
                            null,
                            "basicDBMapper",
                            null,
                            nameResolver,
                            handler,
                            null,
                            null,
                            chosenStrategy, dbQueryEngine, statistics);
        } catch (RDBMSProviderIOException e) {
            throw new RuntimeException(e);
        }

        // adding the mapper to the cache
        dataFetcher.setMapper(mapper);
    }


    @Override
    public GraphData fetchData(DataSourceInfo datasource, String query, int limit) {


        query = fixQuery(datasource, query);

        log.info("query datasource {} with '{}'", datasource.getId(), query);
        // preparing the mapper in the data fetcher

        prepareMapperAndDataFetcher(datasource);

        /*
         * Fetching data
         */

        QueryResult queryResult = null;
        GraphData data;
        List<RelationshipQueryResult> outCountResults = new LinkedList<>();
        List<RelationshipQueryResult> inCountResults = new LinkedList<>();

        try {
            String originalQuery = query;
            String queryWords[] = originalQuery.split(" ");
            String tableName;
            int indexOfTableNameWord = 0;
            for (int i = 0; i < queryWords.length; i++) {
                if (queryWords[i].equalsIgnoreCase("from")) {
                    indexOfTableNameWord = i + 1;
                    break;
                }
            }
            tableName = queryWords[indexOfTableNameWord];
            Entity entity = dataFetcher.getMapper().getEntityByNameIgnoreCase(tableName);

            final Boolean isAggregationEnabled = Optional.ofNullable(datasource.isAggregationEnabled()).orElse(false);
            if (isAggregationEnabled && entity.isAggregableJoinTable()) {
                VertexType aggregatedVertexType = this.dataFetcher.getMapper().getVertexTypeByEntity(entity);
                AggregatorEdge edgeType = this.dataFetcher.getMapper().getAggregatorEdgeByJoinVertexTypeName(aggregatedVertexType.getName());
                throw new RDBMSProviderAggregationException("Wrong query content: "
                        + "the requested table was aggregated into the " + edgeType.getEdgeType().getName() + " edge class.");
            }

            queryResult = dbQueryEngine.scanTableAndOrder(query, limit, entity, datasource);

            if (isAggregationEnabled) {

                Map<String, List<RelationshipQueryResult>> direction2countQueryResults = getRelationshipsCountAggregationCase(datasource, entity);

                // getting out relationships counts
                outCountResults = direction2countQueryResults.get("out");

                // getting in relationships counts
                inCountResults = direction2countQueryResults.get("in");
            } else {

                // getting out relationships counts
                outCountResults = getOutRelationshipsCount(datasource, entity);

                // getting in relationships counts
                inCountResults = getInRelationshipsCount(datasource, entity);
            }

            data = dataFetcher.mapResultSet(queryResult, entity, outCountResults, inCountResults);

        } catch (Exception e) {
            throw new RDBMSProviderRuntimeException(e);
        } finally {
            // closing resultset, connection and statement
            if (queryResult != null) {
                queryResult.close();
            }
            for (RelationshipQueryResult outRelationshipsQueryResult : outCountResults) {
                if (outRelationshipsQueryResult != null) {
                    outRelationshipsQueryResult.close();
                }
            }
            for (RelationshipQueryResult inRelationshipsQueryResult : inCountResults) {
                if (inRelationshipsQueryResult != null) {
                    inRelationshipsQueryResult.close();
                }
            }
        }
        dbQueryEngine.close();
        return data;
    }

    /**
     * Fix the query adding missing columns, if needed.
     * <p>
     * At the moment adds "row_index" column when the datasource is a DataWorld dataset
     *
     * @param datasource data source
     * @param query      the original query
     * @return the fixed query
     */
    private String fixQuery(DataSourceInfo datasource, String query) {
        try {
            final Select select = (Select) CCJSqlParserUtil.parse(query);
            PlainSelect plain = (PlainSelect) select.getSelectBody();
            if (datasource.getType().equals("RDBMS_DATA_WORLD")) {


                boolean notFound = plain.getSelectItems()
                        .stream()
                        .filter(item -> item instanceof SelectExpressionItem)
                        .map(item -> ((SelectExpressionItem) item).getExpression())
                        .filter(expression -> expression instanceof Column)
                        .map(column -> ((Column) column).getColumnName())
                        .filter(name -> name.equals("raw_index"))
                        .collect(Collectors.toList())
                        .isEmpty();
                if (notFound) {
                    log.debug("fixing query to DW, add 'row_index' column");
                    plain.addSelectItems(new SelectExpressionItem(new Column("row_index")));
                }


            }
            query = plain.toString();
        } catch (JSQLParserException e) {
            //NOOP
        }
        return query;
    }


    @Override
    public GraphData expand(DataSourceInfo datasource,
                            String[] roots,
                            final String direction,
                            String edgeClassName,
                            int maxTraversal) {

        if (StringUtils.isEmpty(edgeClassName)) {
            throw new RDBMSProviderOperationNotAllowedException("Cannot perform a traverse all operation over a Relational Datasource.");
        }

        // preparing the mapper in the data fetcher
        prepareMapperAndDataFetcher(datasource);

        /*
         * Fetching data
         */
        QueryResult queryResult = null;
        List<RelationshipQueryResult> outCountResults = new LinkedList<>();
        List<RelationshipQueryResult> inCountResults = new LinkedList<>();
        List<GraphData> graphDataCollection = new LinkedList<>();

        try {

            EdgeType edgeClass = dataFetcher.getMapper().getGraphModel().getEdgeTypeByName(edgeClassName);

            if (!edgeClass.isAggregatorEdge()) {

                // edge class does not correspond to an aggregator edge
                List<Relationship> mappedRelationships = dataFetcher.getMapper().getEdgeType2relationships().get(edgeClass);
                Set<Entity> rootEntitiesInQuery = new HashSet<>();
                for (String rootId : roots) {
                    int entityId = Integer.parseInt(rootId.split("_")[0]);
                    rootEntitiesInQuery.add(dataFetcher.getMapper().getDataBaseSchema().getEntityByPosition(entityId));
                }

                // filtering in just the relationships with the chosen direction
                mappedRelationships = mappedRelationships.stream().filter(r -> {
                    Entity rootEntity = null;
                    if (direction.equals("in")) {
                        rootEntity = r.getParentEntity();
                    } else if (direction.equals("out")) {
                        rootEntity = r.getForeignEntity();
                    }
                    return rootEntitiesInQuery.contains(rootEntity);
                }).collect(Collectors.toList());

                List<Attribute> filteringColumns = null;

                for (Relationship currRelationship : mappedRelationships) {
                    Entity enteringEntity = null;
                    Entity rootEntity = null;
                    if (direction.equals("in")) {
                        enteringEntity = currRelationship.getForeignEntity();
                        rootEntity = currRelationship.getParentEntity();
                        filteringColumns = currRelationship.getFromColumns();
                    } else if (direction.equals("out")) {
                        enteringEntity = currRelationship.getParentEntity();
                        rootEntity = currRelationship.getForeignEntity();
                        filteringColumns = currRelationship.getToColumns();
                    }

                    // build and perform the query, then collect the result

                    //filtering nodes contained in the correspondent tables involved in the current relationship
                    final int rootEntityId = rootEntity.getSchemaPosition();
                    final int enteringEntityId = enteringEntity.getSchemaPosition();
                    List<String> rootNodeIds = Arrays.asList(roots).stream().filter(id -> {
                        int tableId = Integer.parseInt(id.split("_")[0]);
                        return tableId == rootEntityId || tableId == enteringEntityId;
                    }).map(id -> id.substring(id.indexOf("_") + 1))   // cleaning ids to get the original ones back
                            .collect(Collectors.toList());

                    queryResult = dbQueryEngine
                            .expandRelationship(enteringEntity, rootEntity, currRelationship.getFromColumns(), currRelationship.getToColumns(),
                                    rootNodeIds, direction, datasource);

                    List<String> newNodeIds = new ArrayList<>(rootNodeIds);

                    String[] idParts = rootNodeIds.get(0).split("_");
                    if (idParts.length > 1) {
                        // extract just the required id from the composite rootNodesId
                        Integer idTargetIndexToExtract = null;

                        if (direction.equals("out")) {
                            // look for the pk column in the fromColumn
                            idTargetIndexToExtract = currRelationship.getFromColumns().get(0).getOrdinalPosition() - 1;
                        } else if (direction.equals("in")) {
                            // look for the pk column in the toColumn
                            idTargetIndexToExtract = currRelationship.getToColumns().get(0).getOrdinalPosition() - 1;
                        }
                        if (idTargetIndexToExtract != null) {
                            final int index = idTargetIndexToExtract.intValue();
                            newNodeIds = newNodeIds.stream().
                                    map(id -> {
                                        String[] splits = id.split("_");
                                        return splits[index];
                                    }).collect(Collectors.toList());
                        }
                    }

                    // getting out relationships counts
                    outCountResults = getOutRelationshipsCount(datasource, enteringEntity, filteringColumns, newNodeIds);

                    // getting in relationships counts
                    inCountResults = getInRelationshipsCount(datasource, enteringEntity, filteringColumns, newNodeIds);

                    GraphData enteringNodesGraphData = dataFetcher
                            .mapResultSet(queryResult, enteringEntity, outCountResults, inCountResults);
                    graphDataCollection.add(enteringNodesGraphData);

                    // building the edges
                    GraphData enteringEdgesGraphData = dataFetcher
                            .buildEdgesFromJoinResultAndRelationship(queryResult, currRelationship, direction);
                    graphDataCollection.add(enteringEdgesGraphData);
                }
            } else {
                // aggregation case

                // looking for the edge class among the aggregator edges
                edgeClass = dataFetcher.getMapper().getAggregatorEdgeByEdgeTypeName(edgeClassName).getEdgeType();

                VertexType joinVertexType = dataFetcher.getMapper().getJoinVertexTypeByAggregatorEdge(edgeClass.getName());
                Entity joinTable = dataFetcher.getMapper().getEntityByVertexType(joinVertexType);
                List<CanonicalRelationship> joinTableRelationships = new LinkedList<>(joinTable.getOutCanonicalRelationships());
                if (joinTableRelationships.size() != 2) {
                    throw new RDBMSProviderAggregationException("Wrong relationships mapping: "
                            + "the aggregated " + joinTable.getName() + " join table does not have 2 out relationships to represent the N-N relationship.");
                }
                Relationship firstJoinTableRelationship = null;
                Relationship secondJoinTableRelationship = null;
                Entity firstExternalEntity = null;  // the external entity involved in the first join with the join table
                Entity secondExternalEntity = null;  // the external entity involved in the second join with the join table
                if (direction.equals("out")) {
                    firstJoinTableRelationship = joinTableRelationships.get(0);
                    secondJoinTableRelationship = joinTableRelationships.get(1);
                    firstExternalEntity = firstJoinTableRelationship.getParentEntity();
                    secondExternalEntity = secondJoinTableRelationship.getParentEntity();

                } else if (direction.equals("in")) {
                    firstJoinTableRelationship = joinTableRelationships.get(1);
                    secondJoinTableRelationship = joinTableRelationships.get(0);
                    firstExternalEntity = firstJoinTableRelationship.getParentEntity();
                    secondExternalEntity = secondJoinTableRelationship.getParentEntity();
                }

                // cleaning ids to get the original ones back
                List<String> rootNodeIds = Arrays.asList(roots).stream()
                        .map(id -> id.substring(id.indexOf("_") + 1))
                        .collect(Collectors.toList());

                // first external entity join: getting join table's records
                QueryResult joinTableRecordsResult = dbQueryEngine
                        .expandRelationship(joinTable, firstExternalEntity, firstJoinTableRelationship.getFromColumns(), firstJoinTableRelationship.getToColumns(),
                                rootNodeIds, direction, datasource);

                // building the edges from the join table records
                GraphData enteringEdgesGraphData = dataFetcher
                        .buildEdgesFromJoinTableRecords(joinTableRecordsResult, edgeClassName, firstExternalEntity, joinTable, secondExternalEntity, direction);
                graphDataCollection.add(enteringEdgesGraphData);

                // new root ids: we need the ids from the join table records

                rootNodeIds = enteringEdgesGraphData.getEdges().stream()
                        .map(cytoData -> {
                            String id = cytoData.getData().getId();
                            id = id.substring(id.indexOf("_") + 1);
                            return id;
                        }).collect(Collectors.toList());

                // second external entity join: getting entering entity's records
                queryResult = dbQueryEngine
                        .expandRelationship(secondExternalEntity, joinTable, secondJoinTableRelationship.getFromColumns(), secondJoinTableRelationship.getToColumns(),
                                rootNodeIds, direction, datasource);

                Map<String, List<RelationshipQueryResult>> direction2countQueryResults = getRelationshipsCountAggregationCase(datasource, secondExternalEntity);

                // getting out relationships counts
                outCountResults = direction2countQueryResults.get("out");

                // getting in relationships counts
                inCountResults = direction2countQueryResults.get("in");

                GraphData enteringNodesGraphData = dataFetcher
                        .mapResultSet(queryResult, secondExternalEntity, outCountResults, inCountResults);
                graphDataCollection.add(enteringNodesGraphData);

            }

        } catch (Exception e) {
            throw new RDBMSProviderRuntimeException(e);
        } finally {
            // closing resultset, connection and statement
            if (queryResult != null) {
                queryResult.close();
            }
            for (RelationshipQueryResult outRelationshipsQueryResult : outCountResults) {
                if (outRelationshipsQueryResult != null) {
                    outRelationshipsQueryResult.close();
                }
            }
            for (RelationshipQueryResult inRelationshipsQueryResult : inCountResults) {
                if (inRelationshipsQueryResult != null) {
                    inRelationshipsQueryResult.close();
                }
            }
        }

        dbQueryEngine.close();
        final GraphData graphData = collectGraphDatasInSingle(graphDataCollection);
        return graphData;
    }

    @Override
    public GraphData load(DataSourceInfo datasource, String[] ids) {

        // preparing the mapper in the data fetcher
        prepareMapperAndDataFetcher(datasource);

        // order ids by table
        Map<Entity, List<String>> tableName2ids = new LinkedHashMap<Entity, List<String>>();

        for (String currentId : ids) {
            int tableId = Integer.parseInt(currentId.split("_")[0]);
            Entity currentTable = dataFetcher.getMapper().getEntityBySchemaPosition(tableId);
            if (!tableName2ids.containsKey(currentTable)) {
                List<String> currentIds = new LinkedList<String>();
                tableName2ids.put(currentTable, currentIds);
            }
            currentId = currentId.substring(currentId.indexOf("_") + 1);
            tableName2ids.get(currentTable).add(currentId);
        }

        List<String> queryDtos = dbQueryEngine.buildLoadQueries(tableName2ids);

        List<GraphData> graphDataCollection = new LinkedList<GraphData>();
        for (String currentQueryDto : queryDtos) {
            GraphData currGraphData = fetchData(datasource, currentQueryDto, ids.length);
            graphDataCollection.add(currGraphData);
        }
        dbQueryEngine.close();
        final GraphData graphData = collectGraphDatasInSingle(graphDataCollection);
        return graphData;
    }

    @Override
    public GraphData loadFromClass(DataSourceInfo datasource, String className, int limit) {
        String query = "select " + className + ".* from " + className + " limit " + limit;
        return fetchData(datasource, query, limit);
    }

    @Override
    public GraphData loadFromClass(DataSourceInfo datasource, String className, String propName, String propValue, int limit) {
        String query = "select " + className + ".* from " + className + " where " + propName + " = '" + propValue + "' limit " + limit;

        return fetchData(datasource, query, limit);
    }

    @Override
    public boolean testConnection(DataSourceInfo datasource) {

        try (Connection connection = DBSourceConnection.getConnection(datasource)) {
            log.info("connection works fine:: '{}' ", datasource.getId());
        } catch (Exception e) {
            throw new RDBMSProviderRuntimeException(e);
        }
        return true;
    }


    /**
     * Auxiliary functions
     */

    private GraphData collectGraphDatasInSingle(List<GraphData> graphDataCollection) {

        // merge all graph data in a single graph data object
        Map<String, Map<String, Object>> nodeClasses = new LinkedHashMap<>();
        Map<String, Map<String, Object>> edgeClasses = new LinkedHashMap<>();
        Set<CytoData> nodes = new LinkedHashSet<CytoData>();
        Set<CytoData> edges = new LinkedHashSet<CytoData>();

        for (GraphData currentGraphData : graphDataCollection) {
            nodes.addAll(currentGraphData.getNodes());
            edges.addAll(currentGraphData.getEdges());
            nodeClasses.putAll(currentGraphData.getNodesClasses());
            edgeClasses.putAll(currentGraphData.getEdgesClasses());
        }

        return new GraphData(nodeClasses, edgeClasses, nodes, edges, false);
    }


    /**
     * Counting algorithms: base case (without aggregation)
     */

    /**
     * Returns a query result containing the joinable records counts for each out relationship and for each record.
     * Input Entity cannot be an aggregated join table.
     *
     * @param datasource the datasource
     * @param entity     the netity
     * @return a list of results
     */

    public List<RelationshipQueryResult> getOutRelationshipsCount(DataSourceInfo datasource, Entity entity) {
        return getOutRelationshipsCount(datasource, entity, null, null);
    }

    public List<RelationshipQueryResult> getOutRelationshipsCount(DataSourceInfo datasource, Entity entity, List<Attribute> filteringColumns, List<String> filteringRootNodeIds) {
        List<RelationshipQueryResult> countResults = new LinkedList<>();

        for (Relationship currentRelationship : entity.getAllOutCanonicalRelationships()) {

            String relationshipName = dataFetcher.getMapper()
                    .getRelationship2edgeType()
                    .get(currentRelationship)
                    .getName(); // we choose the correspondent edgeClass name as relationship name

            final RelationshipQueryResult queryResult;
            try {
                queryResult = dbQueryEngine
                        .performConnectionCountQueryGroupedByElement(currentRelationship, "foreignTable", filteringColumns, filteringRootNodeIds, datasource, relationshipName);
                countResults.add(queryResult);
            } catch (SQLException e) {
                log.error("unable to get OUT relationship count for :: " + currentRelationship, e);
            }
        }
        return countResults;
    }


    /**
     * Returns a query result containing the joinable records counts for each in relationship and for each record.
     * Input Entity cannot be an aggregated join table.
     *
     * @param datasource the datasource
     * @param entity     the entity
     * @return list of results
     */

    public List<RelationshipQueryResult> getInRelationshipsCount(DataSourceInfo datasource, Entity entity) {
        return getInRelationshipsCount(datasource, entity, null, null);
    }

    public List<RelationshipQueryResult> getInRelationshipsCount(DataSourceInfo datasource, Entity entity, List<Attribute> filteringColumns, List<String> filteringRootNodeIds) {
        List<RelationshipQueryResult> countResults = new LinkedList<>();

        for (Relationship currentRelationship : entity.getAllInCanonicalRelationships()) {
            String relationshipName = dataFetcher.getMapper()
                    .getRelationship2edgeType()
                    .get(currentRelationship)
                    .getName(); // we choose the correspondent edgeClass name as relationship name

            final RelationshipQueryResult queryResult;
            try {
                queryResult = dbQueryEngine
                        .performConnectionCountQueryGroupedByElement(currentRelationship, "parentTable", filteringColumns, filteringRootNodeIds, datasource, relationshipName);
                countResults.add(queryResult);
            } catch (SQLException e) {
                log.error("unable to get IN relationship count for :: " + currentRelationship, e);

            }
        }
        return countResults;
    }


//      Counting algorithm: aggregation case

    private Map<String, List<RelationshipQueryResult>> getRelationshipsCountAggregationCase(DataSourceInfo datasource, Entity entity) {
        return getRelationshipsCountAggregationCase(datasource, entity, null, null);
    }

    private Map<String, List<RelationshipQueryResult>> getRelationshipsCountAggregationCase(DataSourceInfo datasource, Entity entity, List<Attribute> filteringColumns, List<String> filteringRootNodeIds) {

        List<RelationshipQueryResult> outCountResults = new LinkedList<>();
        List<RelationshipQueryResult> inCountResults = new LinkedList<>();


        /*
         * Out Relationships
         */


        for (Relationship currentRelationship : entity.getAllOutCanonicalRelationships()) {

            String relationshipName = null;
            String direction = null;
            Entity currParentEntity = currentRelationship.getParentEntity();
            if (currParentEntity.isAggregableJoinTable()) {
                // maybe this case is never actual, reported as is symmetrical with the in-relationship-case
                // perform counting in the same way but change the edgeName: use the aggregator edge name
                VertexType outVertexType = this.dataFetcher.getMapper().getVertexTypeByEntity(currentRelationship.getForeignEntity());
                VertexType aggregatedVertexType = this.dataFetcher.getMapper().getVertexTypeByEntity(currParentEntity);
                AggregatorEdge aggregatorEdge = this.dataFetcher.getMapper().getAggregatorEdgeByJoinVertexTypeName(aggregatedVertexType.getName());
                relationshipName = aggregatorEdge.getEdgeType().getName();

                // direction
                if (outVertexType.getName().equals(aggregatorEdge.getOutVertexClassName())) {
                    direction = "out";
                } else {
                    direction = "in";
                }

            } else {
                direction = "out";  // edge direction follows the relationship direction, then is 'out'
                relationshipName = dataFetcher.getMapper()
                        .getRelationship2edgeType()
                        .get(currentRelationship)
                        .getName(); // we choose the correspondent edgeClass name as relationship name
            }

            final RelationshipQueryResult queryResult;
            try {
                queryResult = dbQueryEngine
                        .performConnectionCountQueryGroupedByElement(currentRelationship, "foreignTable", filteringColumns, filteringRootNodeIds, datasource, relationshipName);
                if (direction.equals("out")) {
                    outCountResults.add(queryResult);
                } else {
                    inCountResults.add(queryResult);
                }
            } catch (SQLException e) {
                log.error("unable to get OUT relationship count for :: " + currentRelationship, e);
            }
        }


        /*
         * In relationships
         */


        for (Relationship currentRelationship : entity.getAllInCanonicalRelationships()) {

            String relationshipName = null;
            String direction = null;
            Entity currForeignEntity = currentRelationship.getForeignEntity();
            if (currForeignEntity.isAggregableJoinTable()) {

                // perform counting in the same way but change the edgeName: use the aggregator edge name
                VertexType inVertexType = this.dataFetcher.getMapper().getVertexTypeByEntity(currentRelationship.getParentEntity());
                VertexType aggregatedVertexType = this.dataFetcher.getMapper().getVertexTypeByEntity(currForeignEntity);
                AggregatorEdge aggregatorEdge = this.dataFetcher.getMapper().getAggregatorEdgeByJoinVertexTypeName(aggregatedVertexType.getName());
                relationshipName = aggregatorEdge.getEdgeType().getName();

                // direction
                if (inVertexType.getName().equals(aggregatorEdge.getInVertexClassName())) {
                    direction = "in";
                } else {
                    direction = "out";
                }
            } else {
                direction = "in";  // edge direction follows the relationship direction, then is 'out'
                relationshipName = dataFetcher.getMapper().
                        getRelationship2edgeType().
                        get(currentRelationship)
                        .getName(); // we choose the correspondent edgeClass name as relationship name
            }

            final RelationshipQueryResult queryResult;
            try {
                queryResult = dbQueryEngine
                        .performConnectionCountQueryGroupedByElement(currentRelationship, "parentTable", filteringColumns, filteringRootNodeIds, datasource, relationshipName);
                if (direction.equals("out")) {
                    outCountResults.add(queryResult);
                } else {
                    inCountResults.add(queryResult);
                }
            } catch (SQLException e) {
                log.error("unable to get IN relationship count for :: " + currentRelationship, e);
            }
        }

        Map<String, List<RelationshipQueryResult>> direction2countQueryResults = new HashMap<String, List<RelationshipQueryResult>>();
        direction2countQueryResults.put("out", outCountResults);
        direction2countQueryResults.put("in", inCountResults);
        return direction2countQueryResults;
    }

    @NotNull
    @Override
    public Set<String> supportedDataSourceTypes() {
        return Sets.newHashSet("RDBMS_POSTGRESQL",
                "RDBMS_MYSQL",
                "RDBMS_MSSQLSERVER",
                "RDBMS_HSQL",
                "RDBMS_ORACLE",
                "RDBMS_DATA_WORLD");
    }

    @NotNull
    @Override
    public GraphData relations(@NotNull DataSourceInfo dataSource, @NotNull String[] fromIds, @NotNull String[] edgesLabel, @NotNull String[] toIds, int maxTraversal) {
        return null;
    }
}
