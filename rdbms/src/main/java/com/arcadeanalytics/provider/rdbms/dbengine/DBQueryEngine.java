package com.arcadeanalytics.provider.rdbms.dbengine;

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

import com.arcadeanalytics.provider.DataSourceInfo;
import com.arcadeanalytics.provider.rdbms.factory.QueryBuilderFactory;
import com.arcadeanalytics.provider.rdbms.model.dbschema.Attribute;
import com.arcadeanalytics.provider.rdbms.model.dbschema.Entity;
import com.arcadeanalytics.provider.rdbms.model.dbschema.HierarchicalBag;
import com.arcadeanalytics.provider.rdbms.model.dbschema.PrimaryKey;
import com.arcadeanalytics.provider.rdbms.model.dbschema.Relationship;
import com.arcadeanalytics.provider.rdbms.persistence.util.DBSourceConnection;
import com.arcadeanalytics.provider.rdbms.persistence.util.QueryResult;
import com.arcadeanalytics.provider.rdbms.persistence.util.RelationshipQueryResult;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of DataSourceQueryEngine. It executes the necessary queries for the source DB records fetching.
 *
 * @author Gabriele Ponzi
 */

public class DBQueryEngine implements DataSourceQueryEngine {

    private static final Logger log = LoggerFactory.getLogger(DBQueryEngine.class);

    private final QueryBuilder queryBuilder;
    private final DataSourceInfo dataSource;
    private final int maxElements;
    private final Connection dbConnection;

    public DBQueryEngine(DataSourceInfo dataSource, int maxElements) {
        this.dataSource = dataSource;
        this.maxElements = maxElements;
        QueryBuilderFactory queryBuilderFactory = new QueryBuilderFactory();
        this.queryBuilder = queryBuilderFactory.buildQueryBuilder(dataSource.getType());
        dbConnection = DBSourceConnection.getConnection(dataSource);

    }

    public Connection getDbConnection() {
        return dbConnection;
    }

    public QueryResult countTableRecords(String currentTableName, String currentTableSchema) throws SQLException {

        String query = queryBuilder.countTableRecords(currentTableName, currentTableSchema);
        return this.executeQuery(query, dataSource);
    }

    public QueryResult countTableRecords(String tableName) throws SQLException {

        String query = "select count(*) from " + tableName;
        return this.executeQuery(query, dataSource);
    }

    public QueryResult getRecordById(Entity entity, String[] propertyOfKey, String[] valueOfKey) throws SQLException {

        // TODO: queryBuilder fetching
        String query = queryBuilder.getRecordById(entity, propertyOfKey, valueOfKey);
        return executeQuery(query, entity.getDataSource());
    }


    public QueryResult getRecordsByEntity(Entity entity) throws SQLException {

        // TODO: queryBuilder fetching
        String query = queryBuilder.getRecordsByEntity(entity);
        return executeQuery(query, entity.getDataSource());
    }

    public QueryResult getRecordsFromMultipleEntities(List<Entity> mappedEntities, String[][] columns) throws SQLException {

        DataSourceInfo sourceDBInfo = mappedEntities.get(0)
                .getDataSource();   // all the entities belong to the same source database
        // TODO: queryBuilder fetching
        String query = queryBuilder.getRecordsFromMultipleEntities(mappedEntities, columns);
        return executeQuery(query, sourceDBInfo);
    }

    public QueryResult getRecordsFromSingleTableByDiscriminatorValue(String discriminatorColumn, String currentDiscriminatorValue,
                                                                     Entity entity) throws SQLException {

        // TODO: queryBuilder fetching
        String query = queryBuilder
                .getRecordsFromSingleTableByDiscriminatorValue(discriminatorColumn, currentDiscriminatorValue, entity);
        return executeQuery(query, entity.getDataSource());
    }

    public QueryResult getEntityTypeFromSingleTable(String discriminatorColumn, Entity entity, String[] propertyOfKey,
                                                    String[] valueOfKey) throws SQLException {

        // TODO: queryBuilder fetching
        String query = queryBuilder.getEntityTypeFromSingleTable(discriminatorColumn, entity, propertyOfKey, valueOfKey);
        return executeQuery(query, entity.getDataSource());
    }

    public QueryResult buildAggregateTableFromHierarchicalBag(HierarchicalBag bag) throws SQLException {

        // TODO: queryBuilder fetching
        String query = queryBuilder.buildAggregateTableFromHierarchicalBag(bag);
        return executeQuery(query, bag.getSourceDataseInfo());
    }

    public QueryResult executeQuery(String query, DataSourceInfo dataSource) throws SQLException {

        log.debug("query:: {}", query);

        Statement statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ResultSet result = statement.executeQuery(query);

        return new QueryResult(dbConnection, statement, result, query);


    }

    public QueryResult scanTableAndOrder(String query, int limit, Entity entity, DataSourceInfo dataSource) throws SQLException {

        Statement statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

        // setting the threshold for the number of rows
        statement.setMaxRows(limit);

        String limitStatement = null;
        String sqlQuery = query;
        if (sqlQuery.contains("LIMIT")) {
            int index = sqlQuery.lastIndexOf(" LIMIT");
            limitStatement = sqlQuery.substring(index);
            sqlQuery = sqlQuery.substring(0, index);
        }

        // adding ordering to the query by primary key columns
        if (entity.getPrimaryKey() != null) {
            if (!sqlQuery.contains("ORDER BY")) {
                log.info("adds order by");
                sqlQuery += "\nORDER BY ";
                for (Attribute currentAttribute : entity.getPrimaryKey().getInvolvedAttributes()) {
                    sqlQuery += entity.getName() + "." + currentAttribute.getName() + ", ";
                }
                sqlQuery = StringUtils.removeEnd(sqlQuery, ", ");

            }
        }

        if (limitStatement != null) {
            sqlQuery += limitStatement;
        }

        log.debug("sqlQuery :: {} ", sqlQuery);

        ResultSet result = statement.executeQuery(sqlQuery);
        QueryResult queryResult = new QueryResult(dbConnection, statement, result, sqlQuery);
        return queryResult;


    }


    //    /**
//     * Returns a count of 'joinable' records for each record of the pivot table, that is the table for which we are collecting the counts.
//     *
//     * @param relationship
//     * @param pivotTableName
//     * @param filteringIds
//     * @param dataSource
//     * @return relationa result
//     */
    public RelationshipQueryResult performConnectionCountQueryGroupedByElement(Relationship relationship,
                                                                               String pivotTableName,
                                                                               List<Attribute> filteringColumns,
                                                                               List<String> filteringIds,
                                                                               DataSourceInfo dataSource,
                                                                               String relationshipName) throws SQLException {


        Statement statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

        // setting the threshold for the number of rows
        //contract.getMaxElements()
        statement.setMaxRows(1000);

        String query = "select ";
        Entity foreignTable = relationship.getForeignEntity();
        Entity parentTable = relationship.getParentEntity();

        String primaryKeyFields = "";
        if (pivotTableName.equals("foreignTable")) {
            primaryKeyFields = this.buildPrimaryKeyColumnsStatement(foreignTable);
            query += primaryKeyFields + ", ";
        } else if (pivotTableName.equals("parentTable")) {
            primaryKeyFields = this.buildPrimaryKeyColumnsStatement(parentTable);
            query += primaryKeyFields + ", ";
        }
        query += "count(*) as connectionsCount from \n"
                + parentTable.getName() + " join " + foreignTable.getName() + " on ";

        query += buildJoinConditionStatement(relationship.getFromColumns(), relationship.getToColumns(), foreignTable.getName(), parentTable.getName()) + "\n";

        if (filteringColumns != null && filteringIds != null) {
            if (pivotTableName.equals("foreignTable")) {
                query += " and \n" + this.buildIdsINStatement(foreignTable.getName(), filteringColumns, filteringIds) + " ";
            } else if (pivotTableName.equals("parentTable")) {
                query += " and \n" + this.buildIdsINStatement(parentTable.getName(), filteringColumns, filteringIds) + " ";
            }
        }
        query += "group by " + primaryKeyFields + "\n";
        query += "order by " + primaryKeyFields;

        log.debug("query :: {} ", query);
        ResultSet result = statement.executeQuery(query);
        return new RelationshipQueryResult(dbConnection, statement, result, query, relationshipName);


    }

    public RelationshipQueryResult computeRelationshipCardinality(Relationship relationship,
                                                                  DataSourceInfo dataSource,
                                                                  String relationshipName) throws SQLException {

        Statement statement = dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

        // setting the threshold for the number of rows
        statement.setMaxRows(1000);

        String query = "select ";
        Entity foreignTable = relationship.getForeignEntity();
        Entity parentTable = relationship.getParentEntity();

        query += "count(*) as connectionsCount from \n"
                + parentTable.getName() + " join " + foreignTable.getName() + " on ";

        query += buildJoinConditionStatement(relationship.getFromColumns(), relationship.getToColumns(), foreignTable.getName(), parentTable.getName()) + "\n";

        log.debug("query :: {} ", query);
        ResultSet result = statement.executeQuery(query);
        return new RelationshipQueryResult(dbConnection, statement, result, query, relationshipName);
    }


    public QueryResult expandRelationship(Entity enteringEntity,
                                          Entity rootEntity,
                                          List<Attribute> fromColumns,
                                          List<Attribute> toColumns,
                                          List<String> rootNodeIds,
                                          String direction,
                                          DataSourceInfo dataSource) throws SQLException {

        Statement statement = dbConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        // setting the threshold for the number of rows
        //FIXMW
//        statement.setMaxRows(sourceDBInfo.getMaxRowsThreshold());

        String enteringEntityName = enteringEntity.getName();
        String rootEntityName = rootEntity.getName();

        // building the query
        String query = "select ";
        query += this.buildPrimaryKeyColumnsStatement(rootEntity) + ", ";
        for (Attribute currColumn : enteringEntity.getAttributes()) {
            query += enteringEntityName + "." + currColumn.getName() + ", ";
        }
        query = query.substring(0, query.lastIndexOf(", "));
        query += " from \n " + enteringEntityName + " join " + rootEntityName + " on ";
        if (direction.equals("in")) {
            query += this.buildJoinConditionStatement(fromColumns, toColumns, enteringEntityName, rootEntityName) + "\n ";  // tables order not relevant
        } else if (direction.equals("out")) {
            query += this.buildJoinConditionStatement(fromColumns, toColumns, rootEntityName, enteringEntityName) + "\n ";  // tables order not relevant
        }

        Map<Entity, List<String>> tableName2ids = new LinkedHashMap<Entity, List<String>>();
        tableName2ids.put(rootEntity, rootNodeIds);
        query += "where " + this.buildPrimaryKeyIdsINStatement(rootEntity, tableName2ids) + "\n";
        query += "order by " + this.buildPrimaryKeyColumnsStatement(rootEntity);

        log.debug("query :: {} ", query);

        ResultSet result = statement.executeQuery(query);
        QueryResult queryResult = new QueryResult(dbConnection, statement, result, query);
        return queryResult;

    }

    public List<String> buildLoadQueries(Map<Entity, List<String>> tableName2ids) {

        List<String> queryDtos = new ArrayList<>();
        for (Entity currentTable : tableName2ids.keySet()) {
            String currentTableName = currentTable.getName();

            String query = "select * from " + currentTableName + " where ";
            query += buildPrimaryKeyIdsINStatement(currentTable, tableName2ids);
            query += "\n order by ";
            query += buildPrimaryKeyColumnsStatement(currentTable);


            queryDtos.add(query);
        }
        return queryDtos;
    }

    // tables order not relevant
    private String buildJoinConditionStatement(List<Attribute> fromColumns, List<Attribute> toColumns, String foreignTableName, String parentTableName) {
        int numberOfJoinColumns = fromColumns.size();
        String joinCondition = parentTableName + "." + toColumns.get(0).getName() + "=" +
                foreignTableName + "." + fromColumns.get(0).getName();
        if (numberOfJoinColumns > 1) {
            for (int i = 1; i < fromColumns.size(); i++) {
                joinCondition += ", " + parentTableName + "." + toColumns.get(i).getName() + "=" +
                        foreignTableName + "." + fromColumns.get(0);
            }
        }
        return joinCondition;
    }

    private String buildPrimaryKeyColumnsStatement(Entity table) {
        String statement = "";
        String tableName = table.getName();
        for (Attribute a : table.getPrimaryKey().getInvolvedAttributes()) {
            statement += tableName + "." + a.getName() + ", ";
        }
        statement = statement.substring(0, statement.lastIndexOf(", "));
        return statement;
    }

    private String buildPrimaryKeyIdsINStatement(Entity table, Map<Entity, List<String>> tableName2ids) {
        String statement = "";
        String tableName = table.getName();
        PrimaryKey primaryKey = table.getPrimaryKey();

        for (Attribute a : primaryKey.getInvolvedAttributes()) {
            statement += tableName + "." + a.getName() + " in (";
            for (String currentId : tableName2ids.get(table)) {
                String[] currentIdSplit = currentId.split("_");
                statement += "'" + currentIdSplit[a.getOrdinalPosition() - 1] + "', ";
            }
            statement = statement.substring(0, statement.lastIndexOf(", "));
            statement += ") and ";
        }
        statement = statement.substring(0, statement.lastIndexOf(" and "));
        return statement;
    }

    private String buildIdsINStatement(String tableName, List<Attribute> columns, List<String> ids) {
        String statement = "";

        for (Attribute currColumn : columns) {
            statement += tableName + "." + currColumn.getName() + " in (";
            for (String currentId : ids) {
                statement += "'" + currentId + "', ";
            }
            statement = statement.substring(0, statement.lastIndexOf(", "));
            statement += ") and ";
        }
        statement = statement.substring(0, statement.lastIndexOf(" and "));
        return statement;
    }


    public void close() {
        try {
            log.debug("    closing connection");
            dbConnection.close();
        } catch (SQLException e) {
            log.error("", e);
        }

    }
}
