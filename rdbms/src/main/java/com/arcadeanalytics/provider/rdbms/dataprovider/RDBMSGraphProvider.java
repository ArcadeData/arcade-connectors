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

import com.arcadeanalytics.data.Sprite;
import com.arcadeanalytics.data.SpritePlayer;
import com.arcadeanalytics.provider.DataSourceGraphProvider;
import com.arcadeanalytics.provider.DataSourceInfo;
import com.arcadeanalytics.provider.rdbms.context.Statistics;
import com.arcadeanalytics.provider.rdbms.dbengine.DBQueryEngine;
import com.arcadeanalytics.provider.rdbms.exception.RDBMSProviderIOException;
import com.arcadeanalytics.provider.rdbms.exception.RDBMSProviderRuntimeException;
import com.arcadeanalytics.provider.rdbms.factory.DataTypeHandlerFactory;
import com.arcadeanalytics.provider.rdbms.factory.NameResolverFactory;
import com.arcadeanalytics.provider.rdbms.factory.StrategyFactory;
import com.arcadeanalytics.provider.rdbms.mapper.rdbms.ER2GraphMapper;
import com.arcadeanalytics.provider.rdbms.model.dbschema.Attribute;
import com.arcadeanalytics.provider.rdbms.model.dbschema.Entity;
import com.arcadeanalytics.provider.rdbms.model.dbschema.PrimaryKey;
import com.arcadeanalytics.provider.rdbms.model.graphmodel.VertexType;
import com.arcadeanalytics.provider.rdbms.nameresolver.NameResolver;
import com.arcadeanalytics.provider.rdbms.persistence.handler.DBMSDataTypeHandler;
import com.arcadeanalytics.provider.rdbms.persistence.util.DBSourceConnection;
import com.arcadeanalytics.provider.rdbms.persistence.util.QueryResult;
import com.arcadeanalytics.provider.rdbms.strategy.rdbms.AbstractDBMSModelBuildingStrategy;
import com.google.common.collect.Sets;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


public class RDBMSGraphProvider implements DataSourceGraphProvider {

    private final Logger log = LoggerFactory.getLogger(RDBMSGraphProvider.class);

    @Override
    public void provideTo(DataSourceInfo datasource, final SpritePlayer player) {

        Statistics statistics = new Statistics();

        String connectionUrl = DBSourceConnection.createConnectionUrl(datasource);
        try {

            final ER2GraphMapper mapper = prepareMapperAndDataFetcher(datasource, statistics);

            DBQueryEngine dbQueryEngine = new DBQueryEngine(datasource, 300);

            final Map<Entity, Integer> entity2count = countEntities(datasource, mapper);

            for (Map.Entry<Entity, Integer> entry : entity2count.entrySet()) {

                Entity currentEntity = entry.getKey();
                final String tableName = entry.getKey().getName();
                String query = "select * from " + tableName;
                if (datasource.getType().equals("RDBMS_DATA_WORLD")) {
                    query = "select " + tableName + ".*, row_index from " + tableName;
                }
//                String tableName = query.substring(query.lastIndexOf("from ") + 5);
                int count = entry.getValue();
                log.info("fetching data from source'{}', table '{}' with query '{}' - total records '{}' ", connectionUrl, tableName, query, count);

                QueryResult queryResult = dbQueryEngine.executeQuery(query, datasource);
                ResultSet result = queryResult.getResult();

                while (result.next()) {
                    ResultSet currentRecord = result;

                    ResultSetMetaData rsmd = currentRecord.getMetaData();
                    int columnCount = rsmd.getColumnCount();

                    String id = getCytoIdFromPrimaryKey(currentRecord, currentEntity);
                    String vertexClassName = mapper.getVertexTypeByEntity(currentEntity).getName();
                    Sprite document = new Sprite()
                            .add("@class", vertexClassName)
                            .add(com.arcadeanalytics.provider.IndexConstants.ARCADE_ID, id)
                            .add(com.arcadeanalytics.provider.IndexConstants.ARCADE_TYPE, com.arcadeanalytics.provider.IndexConstants.ARCADE_NODE_TYPE);
                    for (int i = 1; i <= columnCount; i++) {
                        document.add(rsmd.getColumnName(i), currentRecord.getObject(i));
                    }
                    player.play(document);
                }
                player.end();

                // releasing the resources
                queryResult.close();
            }
            player.end();

            if (datasource.isAggregationEnabled()) {

                final Map<Entity, Integer> jointTable2count = countJoinTables(datasource, mapper);
                // we have to index just the aggregator edges
                for (Map.Entry<Entity, Integer> entry : jointTable2count.entrySet()) {
                    Entity currentEntity = entry.getKey();
                    if (currentEntity.isAggregableJoinTable()) {
                        String query = "select * from " + entry.getKey().getName();
                        String tableName = query.substring(query.lastIndexOf("from ") + 5);
                        int count = entry.getValue();
                        log.info("fetching data from source'{}', table '{}' with query '{}' - total records '{}' ", connectionUrl, tableName, query, count);

                        QueryResult queryResult = dbQueryEngine.executeQuery(query, datasource);
                        ResultSet result = queryResult.getResult();

                        while (result.next()) {
                            ResultSet currentRecord = result;

                            ResultSetMetaData rsmd = currentRecord.getMetaData();
                            int columnCount = rsmd.getColumnCount();

                            String id = getCytoIdFromPrimaryKey(currentRecord, currentEntity);
                            VertexType vertextype = mapper.getVertexTypeByEntity(currentEntity);
                            String edgeClassName = mapper.getAggregatorEdgeByJoinVertexTypeName(vertextype.getName()).getEdgeType().getName();
                            Sprite document = new Sprite()
                                    .add("@class", edgeClassName)
                                    .add(com.arcadeanalytics.provider.IndexConstants.ARCADE_ID, id)
                                    .add(com.arcadeanalytics.provider.IndexConstants.ARCADE_TYPE, com.arcadeanalytics.provider.IndexConstants.ARCADE_EDGE_TYPE);
                            List<String> pkColumnsNames = currentEntity.getPrimaryKey().getInvolvedAttributes().stream()
                                    .map(attribute -> attribute.getName())
                                    .collect(Collectors.toList());
                            for (int i = 1; i <= columnCount; i++) {
                                if (!pkColumnsNames.contains(rsmd.getColumnName(i))) {
                                    document.add(rsmd.getColumnName(i), currentRecord.getObject(i));
                                }
                            }
                            player.play(document);
                        }
                        player.end();

                        // releasing the resources
                        queryResult.close();
                    }
                }
            }
            player.end();

            dbQueryEngine.close();
        } catch (Exception e) {
            log.error("error while connecting to  " + datasource, e);
        }
    }


    public ER2GraphMapper prepareMapperAndDataFetcher(DataSourceInfo datasource, Statistics statistics) {

        DBQueryEngine dbQueryEngine = new DBQueryEngine(datasource, 300);

        final Boolean aggregate = datasource.isAggregationEnabled();

        String chosenStrategy = aggregate ? "interactive-aggr" : "interactive";

        DataTypeHandlerFactory dataTypeHandlerFactory = new DataTypeHandlerFactory();
        DBMSDataTypeHandler handler = dataTypeHandlerFactory.buildDataTypeHandler(datasource.getType());

        NameResolverFactory nameResolverFactory = new NameResolverFactory();
        NameResolver nameResolver = nameResolverFactory.buildNameResolver("original");

        StrategyFactory strategyFactory = new StrategyFactory();
        try {
            ER2GraphMapper mapper = ((AbstractDBMSModelBuildingStrategy) strategyFactory.buildStrategy(chosenStrategy))
                    .createSchemaMapper(datasource,
                            null,
                            "basicDBMapper",
                            null,
                            nameResolver,
                            handler,
                            null,
                            null,
                            chosenStrategy,
                            dbQueryEngine, statistics);
            return mapper;
        } catch (RDBMSProviderIOException e) {
            throw new RuntimeException(e);
        }

    }


    public Map<Entity, Integer> countEntities(DataSourceInfo datasource, ER2GraphMapper mapper) throws Exception {

        // get all table names
        try (Connection connection = DBSourceConnection.getConnection(datasource)) {
            Map<Entity, Integer> entity2count = new LinkedHashMap<>();


            List<Entity> entities = mapper.getDataBaseSchema().getEntities();
            if (datasource.isAggregationEnabled()) {

                // filtering out the join tables
                entities = entities.stream()
                        .filter(entity -> {
                            if (!entity.isAggregableJoinTable()) {
                                return true;
                            }
                            return false;
                        }).collect(Collectors.toList());
            }

            // Giving db's table names and building all the queries, then adding them to the instance variable
            for (Entity currEntity : entities) {
                String tableName = currEntity.getName();
                String countQuery = "select count(*) from " + tableName;

                Statement countStatement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                ResultSet resultCount = countStatement.executeQuery(countQuery);
                resultCount.next();
                int count = resultCount.getInt(1);
                entity2count.put(currEntity, count);

                resultCount.close();
                countStatement.close();

            }

            return entity2count;
        } catch (SQLException e) {
            throw new RDBMSProviderRuntimeException(e);
        }

    }

    public Map<Entity, Integer> countJoinTables(DataSourceInfo datasource, ER2GraphMapper mapper) throws Exception {


        // get all table names
        try (Connection connection = DBSourceConnection.getConnection(datasource)) {

            Map<Entity, Integer> jointTable2count = new LinkedHashMap<>();
            Statement countStatement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

            Set<VertexType> joinVertices = mapper.getJoinVertex2aggregatorEdges().keySet();
            for (VertexType currJoinVertexType : joinVertices) {
                Entity currEntity = mapper.getEntityByVertexType(currJoinVertexType);
                String tableName = currEntity.getName();
                String countQuery = "select count(*) from " + tableName;
                ResultSet resultCount = countStatement.executeQuery(countQuery);
                resultCount.next();
                int count = resultCount.getInt(1);
                resultCount.close();
                jointTable2count.put(currEntity, count);
            }
            return jointTable2count;
        } catch (SQLException e) {
            throw new RDBMSProviderRuntimeException(e);
        }

    }


    private String getCytoIdFromPrimaryKey(ResultSet sourceRecord, Entity entity) throws SQLException {

        PrimaryKey primaryKey = entity.getPrimaryKey();
        String id = entity.getSchemaPosition() + "_";

        for (Attribute attribute : primaryKey.getInvolvedAttributes()) {
            id += sourceRecord.getString(attribute.getName()) + "_";
        }
        id = id.substring(0, id.lastIndexOf("_"));
        return id;
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
}
