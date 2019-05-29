package com.arcadeanalytics.provider.rdbms.mapper.rdbms;

/*-
 * #%L
 * Arcade Connectors
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

import com.arcadeanalytics.provider.DataSourceInfo;
import com.arcadeanalytics.provider.TypeMapperKt;
import com.arcadeanalytics.provider.rdbms.context.Statistics;
import com.arcadeanalytics.provider.rdbms.dbengine.DBQueryEngine;
import com.arcadeanalytics.provider.rdbms.exception.RDBMSProviderRuntimeException;
import com.arcadeanalytics.provider.rdbms.mapper.rdbms.classmapper.EEClassMapper;
import com.arcadeanalytics.provider.rdbms.mapper.rdbms.classmapper.EVClassMapper;
import com.arcadeanalytics.provider.rdbms.model.dbschema.Attribute;
import com.arcadeanalytics.provider.rdbms.model.dbschema.CanonicalRelationship;
import com.arcadeanalytics.provider.rdbms.model.dbschema.DataBaseSchema;
import com.arcadeanalytics.provider.rdbms.model.dbschema.Entity;
import com.arcadeanalytics.provider.rdbms.model.dbschema.ForeignKey;
import com.arcadeanalytics.provider.rdbms.model.dbschema.PrimaryKey;
import com.arcadeanalytics.provider.rdbms.model.dbschema.Relationship;
import com.arcadeanalytics.provider.rdbms.model.graphmodel.EdgeType;
import com.arcadeanalytics.provider.rdbms.model.graphmodel.ElementType;
import com.arcadeanalytics.provider.rdbms.model.graphmodel.GraphModel;
import com.arcadeanalytics.provider.rdbms.model.graphmodel.ModelProperty;
import com.arcadeanalytics.provider.rdbms.model.graphmodel.VertexType;
import com.arcadeanalytics.provider.rdbms.nameresolver.NameResolver;
import com.arcadeanalytics.provider.rdbms.persistence.handler.DBMSDataTypeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeMap;

/**
 * Implementation of Source2GraphMapper that manages the source DB schema and the destination graph model with their
 * correspondences. It has the responsibility to build in memory the two models: the first is built from the source DB meta-data
 * through the JDBC driver, the second from the source DB schema just created.
 *
 * @author Gabriele Ponzi
 */

public class ER2GraphMapper {

    public final int DEFAULT_CLASS_MAPPER_INDEX = 0;
    // rules
    protected final Map<Entity, List<EVClassMapper>> entity2EVClassMappers;
    protected final Map<VertexType, List<EVClassMapper>> vertexType2EVClassMappers;
    protected final Map<String, List<EEClassMapper>> entity2EEClassMappers;
    protected final Map<String, List<EEClassMapper>> edgeType2EEClassMappers;
    protected final Map<Relationship, EdgeType> relationship2edgeType;
    protected final Map<EdgeType, LinkedList<Relationship>> edgeType2relationships;
    protected final Map<String, Integer> edgeTypeName2count;
    protected final Map<VertexType, AggregatorEdge> joinVertex2aggregatorEdges;
    // supplementary migrationConfigDoc
    protected final DataSourceInfo dataSource;
    private final DBQueryEngine queryEngine;
    private final String executionStrategy;
    private final DBMSDataTypeHandler dataTypeHandler;
    private final NameResolver nameResolver;
    private final Statistics statistics;
    private final Logger log = LoggerFactory.getLogger(ER2GraphMapper.class);
    // graph model
    protected GraphModel graphModel;
    // source model
    protected DataBaseSchema dataBaseSchema;
    // filters
    protected List<String> includedTables;
    protected List<String> excludedTables;

    public ER2GraphMapper(DataSourceInfo dataSource,
                          List<String> includedTables,
                          List<String> excludedTables,
                          DBQueryEngine queryEngine,
                          DBMSDataTypeHandler dataTypeHandler,
                          String executionStrategy,
                          NameResolver nameResolver,
                          Statistics statistics) {
        this.dataSource = dataSource;
        this.queryEngine = queryEngine;
        this.executionStrategy = executionStrategy;
        this.dataTypeHandler = dataTypeHandler;
        this.nameResolver = nameResolver;
        this.statistics = statistics;


        // new maps
        this.entity2EVClassMappers = new IdentityHashMap<>();
        this.vertexType2EVClassMappers = new IdentityHashMap<>();

        this.relationship2edgeType = new IdentityHashMap<>();
        this.edgeType2relationships = new IdentityHashMap<>();
        this.edgeTypeName2count = new TreeMap<>();
        this.joinVertex2aggregatorEdges = new LinkedHashMap<>();
        this.entity2EEClassMappers = new LinkedHashMap<>();
        this.edgeType2EEClassMappers = new LinkedHashMap<>();

        if (includedTables != null)
            this.includedTables = includedTables;
        else
            this.includedTables = new ArrayList<>();

        if (excludedTables != null)
            this.excludedTables = excludedTables;
        else
            this.excludedTables = new ArrayList<>();

        // creating the two empty models
        this.dataBaseSchema = new DataBaseSchema();
        this.graphModel = new GraphModel();

    }

    public GraphModel getGraphModel() {
        return this.graphModel;
    }

    // old map managing
    public void upsertRelationshipEdgeRules(Relationship currentRelationship, EdgeType currentEdgeType) {
        relationship2edgeType.put(currentRelationship, currentEdgeType);

        LinkedList<Relationship> representedRelationships = edgeType2relationships.get(currentEdgeType);
        if (representedRelationships == null) {
            representedRelationships = new LinkedList<>();
        }
        representedRelationships.add(currentRelationship);
        edgeType2relationships.put(currentEdgeType, representedRelationships);
    }

    public void upsertEVClassMappingRules(Entity currentEntity, VertexType currentVertexType, EVClassMapper classMapper) {

        List<EVClassMapper> classMappings = entity2EVClassMappers.get(currentEntity);
        if (classMappings == null) {
            classMappings = new LinkedList<>();
        }
        classMappings.add(classMapper);
        entity2EVClassMappers.put(currentEntity, classMappings);

        classMappings = vertexType2EVClassMappers.get(currentVertexType);
        if (classMappings == null) {
            classMappings = new LinkedList<>();
        }
        classMappings.add(classMapper);
        vertexType2EVClassMappers.put(currentVertexType, classMappings);
    }

    public void upsertEEClassMappingRules(Entity currentEntity, EdgeType currentEdgeType, EEClassMapper classMapper) {

        List<EEClassMapper> classMappings = entity2EEClassMappers.get(currentEntity.getName());
        if (classMappings == null) {
            classMappings = new LinkedList<>();
        }
        classMappings.add(classMapper);
        entity2EEClassMappers.put(currentEntity.getName(), classMappings);

        classMappings = edgeType2EEClassMappers.get(currentEdgeType.getName());
        if (classMappings == null) {
            classMappings = new LinkedList<EEClassMapper>();
        }
        classMappings.add(classMapper);
        edgeType2EEClassMappers.put(currentEdgeType.getName(), classMappings);
    }

    public List<EVClassMapper> getEVClassMappersByVertex(VertexType vertexType) {
        return vertexType2EVClassMappers.get(vertexType);
    }

    public List<EVClassMapper> getEVClassMappersByEntity(Entity entity) {
        return entity2EVClassMappers.get(entity);
    }

    public List<EEClassMapper> getEEClassMappersByEntity(Entity entity) {
        return entity2EEClassMappers.get(entity.getName());
    }

    public List<EEClassMapper> getEEClassMappersByEdge(EdgeType edgeType) {
        return edgeType2EEClassMappers.get(edgeType.getName());
    }

    public Map<Entity, List<EVClassMapper>> getEntity2EVClassMappers() {
        return entity2EVClassMappers;
    }

    public Map<VertexType, List<EVClassMapper>> getVertexType2EVClassMappers() {
        return vertexType2EVClassMappers;
    }

    public Map<String, List<EEClassMapper>> getEntity2EEClassMappers() {
        return entity2EEClassMappers;
    }

    public Map<String, List<EEClassMapper>> getEdgeType2EEClassMappers() {
        return edgeType2EEClassMappers;
    }

    public String getAttributeByPropertyAboveMappers(String propertyName, List<EVClassMapper> classMappers) {

        for (EVClassMapper currClassMapper : classMappers) {
            String attributeName = currClassMapper.getAttributeByProperty(propertyName);
            if (attributeName != null) {
                return attributeName;
            }
        }
        return null;
    }


    /**
     * MACRO EXECUTION BLOCK: BUILD SOURCE DATABASE SCHEMA
     * Builds the database schema and the rules for the mapping with the graph model through 3 micro execution blocks:
     * - Build Entities
     * - Build Out-Relationships
     * - Build In-Relationships
     */

    public void buildSourceDatabaseSchema() {
        statistics.startWork1Time = new Date();
        statistics.runningStepNumber = 1;


        try {
            Connection connection = queryEngine.getDbConnection();
            DatabaseMetaData databaseMetaData = connection.getMetaData();

            log.debug("databaseMetaData:: {} ", databaseMetaData);
            /*
             *  General DB Info and table filtering
             */

            dataBaseSchema.setMajorVersion(databaseMetaData.getDatabaseMajorVersion());
            dataBaseSchema.setMinorVersion(databaseMetaData.getDatabaseMinorVersion());
            dataBaseSchema.setDriverMajorVersion(databaseMetaData.getDriverMajorVersion());
            dataBaseSchema.setDriverMinorVersion(databaseMetaData.getDriverMinorVersion());
            dataBaseSchema.setProductName(databaseMetaData.getDatabaseProductName());
            dataBaseSchema.setProductVersion(databaseMetaData.getDatabaseProductVersion());

            /*
             *  Entity building
             */


            int numberOfTables = buildEntities(databaseMetaData, connection);

            /*
             *  Building Out-relationships
             */

            buildOutRelationships(databaseMetaData, numberOfTables);


            /*
             *  Building In-relationships
             */

            buildInRelationships();

        } catch (SQLException e) {
            throw new RDBMSProviderRuntimeException(e);
        }

        statistics.runningStepNumber = -1;
    }

    /**
     * MICRO EXECUTION BLOCK: BUILD SOURCE DATABASE SCHEMA - BUILD ENTITIES
     * Builds the Entities starting from the source database metadata.
     *
     * @param databaseMetaData
     * @param sourceDBConnection
     * @return
     * @throws SQLException
     */

    private int buildEntities(DatabaseMetaData databaseMetaData, Connection sourceDBConnection) throws SQLException {

        Map<String, String> tablesName2schema = new LinkedHashMap<>();

        String tableCatalog = null;
        String tableSchemaPattern = null;
        String tableNamePattern = null;
        String[] tableTypes = {"TABLE"};
        if (dataSource.getType().equals("RDBMS_ORACLE")) {
            ResultSet schemas = databaseMetaData.getSchemas();
            while (schemas.next()) {
                if (schemas.getString(1).equalsIgnoreCase(dataSource.getUsername())) {
                    tableSchemaPattern = schemas.getString(1);
                    break;
                }
            }
        }

        /**
         * MySQL Hack: retrieving info about tables contained in the schema called with the database name
         */
        if (dataSource.getType().equals("RDBMS_MYSQL")) {
            tableCatalog = dataSource.getDatabase();
            if (tableSchemaPattern == null) {
                tableSchemaPattern = dataSource.getDatabase();
            }
        }
        ResultSet resultTable = databaseMetaData.getTables(tableCatalog, tableSchemaPattern, tableNamePattern, tableTypes);

        // Giving db's table names
        while (resultTable.next()) {
            String tableSchema = resultTable.getString("TABLE_SCHEM");
            String tableName = resultTable.getString("TABLE_NAME");

            if (isTableAllowed(tableName))  // filtering tables according to "include-list" and "exclude-list"
                tablesName2schema.put(tableName, tableSchema);
        }

        int numberOfTables = tablesName2schema.size();
        statistics.totalNumberOfEntities = numberOfTables;

        // closing resultTable
        closeCursor(resultTable);
        log.debug("{} tables found:: {} ", numberOfTables, tablesName2schema);

        // Variables for records counting
        Statement statement = sourceDBConnection.createStatement();
        int totalNumberOfRecord = 0;

        int iteration = 1;
        for (String currentTableName : tablesName2schema.keySet()) {

            // Counting current-table's record
            String currentTableSchema = tablesName2schema.get(currentTableName);
            log.debug("Building '{}' entity on schema {} ({}/{})...", currentTableName, currentTableSchema, iteration, numberOfTables);

//            QueryResult queryResult = dbQueryEngine.countTableRecords(currentTableName, currentTableSchema);
//
//            ResultSet currentTableRecordAmount = queryResult.getResult();
//            if (currentTableRecordAmount.next()) {
//                totalNumberOfRecord += currentTableRecordAmount.getInt(1);
//            }
//            closeCursor(currentTableRecordAmount);

            // creating entity
            Entity currentEntity = new Entity(currentTableName, currentTableSchema, dataSource);
            currentEntity.setSchemaPosition(iteration);

            // adding attributes and primary keys
            PrimaryKey pKey = new PrimaryKey(currentEntity, new ArrayList<>());

            String columnCatalog = null;
            String columnSchemaPattern = null;
            String columnNamePattern = null;

            String primaryKeyCatalog = null;
            String primaryKeySchema = currentTableSchema;

            /**
             * MySQL Hack
             */
            if (dataSource.getType().equals("RDBMS_MYSQL")) {
                primaryKeyCatalog = dataSource.getDatabase();
                if (primaryKeySchema == null) {
                    primaryKeySchema = dataSource.getDatabase();
                }
            }

            ResultSet resultColumns = databaseMetaData.getColumns(columnCatalog, columnSchemaPattern, currentTableName, columnNamePattern);

            log.debug("primaryKeyCatalog ::: {} ", primaryKeySchema);
            ResultSet resultPrimaryKeys = databaseMetaData.getPrimaryKeys(primaryKeyCatalog, primaryKeySchema, currentTableName);

            List<String> currentPrimaryKeys = getPrimaryKeysFromResulset(resultPrimaryKeys);

            while (resultColumns.next()) {
                Attribute currentAttribute = new Attribute(resultColumns.getString("COLUMN_NAME")
                        , resultColumns.getInt("ORDINAL_POSITION")
                        , resultColumns.getString("TYPE_NAME")
                        , currentEntity);
                currentEntity.addAttribute(currentAttribute);

                // if the current attribute is involved in the primary key, it will be added to the attributes of pKey.
                if (currentPrimaryKeys.contains(currentAttribute.getName())) {
                    pKey.addAttribute(currentAttribute);
                }
            }

            /**
             *dataWorkd Hack
             */
            if (dataSource.getType().equals("RDBMS_DATA_WORLD")) {

                currentPrimaryKeys.add("row_index");

                final OptionalInt maxOrdinal = currentEntity.getAttributes().stream()
                        .mapToInt(a -> a.getOrdinalPosition())
                        .max();

                Attribute currentAttribute = new Attribute("row_index"
                        , maxOrdinal.getAsInt() + 1
                        , " http://www.w3.org/2001/XMLSchema#integer"
                        , currentEntity);
                currentEntity.addAttribute(currentAttribute);

                pKey.addAttribute(currentAttribute);

            }


            closeCursor(resultColumns);
            closeCursor(resultPrimaryKeys);

            currentEntity.setPrimaryKey(pKey);

            // if the primary key doesn't involve any attribute, a warning message is generated
            if (pKey.getInvolvedAttributes().size() == 0)
                log.warn("It's not declared a primary key for the Entity " + currentEntity.getName()
                        + ", might lead to issues during the migration or the sync executions " + "(the first importing is quite safe).");

            // adding entity to db schema
            dataBaseSchema.getEntities().add(currentEntity);

            iteration++;
            log.debug("Entity {} built.", currentTableName);
            statistics.builtEntities++;
            statistics.totalNumberOfRecords = totalNumberOfRecord;

            // releasing resources
//            queryResult.closeAll();
        }
        statement.close();

        return numberOfTables;
    }

    /**
     * MICRO EXECUTION BLOCK: BUILD SOURCE DATABASE SCHEMA - BUILD OUT-RELATIONSHIPS
     * Builds the references to the "Out Relationships" starting from the source database metadata.
     *
     * @param databaseMetaData metadata of database
     * @param numberOfTables   number of tables
     * @throws SQLException in case of error
     */

    private void buildOutRelationships(DatabaseMetaData databaseMetaData, int numberOfTables) throws SQLException {


        int iteration = 1;
        for (Entity currentForeignEntity : dataBaseSchema.getEntities()) {

            String currentForeignEntityName = currentForeignEntity.getName();
            String foreignSchema = currentForeignEntity.getSchemaName();
            log.debug("Building OUT relationships starting from '{}' entity ({}/{})...", currentForeignEntityName, iteration, numberOfTables);

            String foreignCatalog = null;
            ResultSet resultForeignKeys = databaseMetaData.getImportedKeys(foreignCatalog, foreignSchema, currentForeignEntityName);

            // copy of Resultset in a HashLinkedMap
            List<LinkedHashMap<String, String>> currentEntityRelationships1 = fromResultSetToList(resultForeignKeys);
            List<LinkedHashMap<String, String>> currentEntityRelationships2 = new LinkedList<LinkedHashMap<String, String>>();

            for (LinkedHashMap<String, String> row : currentEntityRelationships1) {
                currentEntityRelationships2.add(row);
            }

            closeCursor(resultForeignKeys);

            Iterator<LinkedHashMap<String, String>> it1 = currentEntityRelationships1.iterator();
            Iterator<LinkedHashMap<String, String>> it2 = currentEntityRelationships2.iterator();

            while (it1.hasNext()) {
                LinkedHashMap<String, String> currentExternalRow = it1.next();

                // current row has Key_Seq equals to '2' then algorithm is finished and is stopped
                if (currentExternalRow.get("key_seq").equals("2")) {
                    break;
                }

                // the original relationship is fetched from the record through the 'parent table' and the 'key sequence numbers'
                String currentParentTableName = currentExternalRow.get("pktable_name");
                int currentKeySeq = Integer.parseInt(currentExternalRow.get("key_seq"));

                // building each single relationship from each correspondent foreign key
                Entity currentParentTable = dataBaseSchema.getEntityByName(currentParentTableName);
                ForeignKey currentFk = new ForeignKey(currentForeignEntity, new ArrayList<>());
                while (it2.hasNext()) {
                    LinkedHashMap<String, String> row = it2.next();
                    if (row.get("pktable_name").equals(currentParentTableName) && Integer.parseInt(row.get("key_seq")) == currentKeySeq) {
                        currentFk.addAttribute(currentForeignEntity.getAttributeByName((String) row.get("fkcolumn_name")));
                        it2.remove();
                        currentKeySeq++;
                    }
                }

                // iterator reset
                it2 = currentEntityRelationships2.iterator();

                // searching correspondent primary key
                PrimaryKey currentPk = dataBaseSchema.getEntityByName(currentParentTableName).getPrimaryKey();

                // adding foreign key to the entity and the relationship, and adding the foreign key to the 'foreign entity'
                CanonicalRelationship currentRelationship = new CanonicalRelationship(currentForeignEntity, currentParentTable, currentFk, currentPk);
                currentForeignEntity.getForeignKeys().add(currentFk);

                // adding the relationship to the db schema
                dataBaseSchema.getCanonicalRelationships().add(currentRelationship);
                // adding relationship to the current entity
                currentForeignEntity.getOutCanonicalRelationships().add(currentRelationship);
                // updating statistics
                statistics.builtRelationships += 1;
            }

            iteration++;
            log.debug("OUT Relationships from {} built.", currentForeignEntityName);
            statistics.entitiesAnalyzedForRelationship++;
        }

        statistics.totalNumberOfRelationships = dataBaseSchema.getCanonicalRelationships().size();
    }

    /**
     * MICRO EXECUTION BLOCK: BUILD SOURCE DATABASE SCHEMA - BUILD IN-RELATIONSHIPS
     * Builds the references to the "In Relationships" starting from the references to the "Out Relationships".
     */

    private void buildInRelationships() {

        int iteration = 1;
        log.debug("Connecting IN relationships...");

        for (Relationship currentRelationship : dataBaseSchema.getCanonicalRelationships()) {
            Entity currentInEntity = getDataBaseSchema().getEntityByName(currentRelationship.getParentEntity().getName());
            currentInEntity.getInCanonicalRelationships().add((CanonicalRelationship) currentRelationship);
        }

        log.debug("IN relationships built.");
    }

    private List<String> getPrimaryKeysFromResulset(ResultSet resultPrimaryKeys) throws SQLException {

        List<String> currentPrimaryKeys = new LinkedList<String>();

        while (resultPrimaryKeys.next()) {
            currentPrimaryKeys.add(resultPrimaryKeys.getString(4));
        }
        return currentPrimaryKeys;
    }

    private void closeCursor(ResultSet result) {
        try {
            if (result != null)
                result.close();
        } catch (SQLException e) {
            log.error("", e);
        }
    }


    /*
     * Transforms a ResultSet in a List, filtering relationships according to "include/exclude-lists"
     */

    private List<LinkedHashMap<String, String>> fromResultSetToList(ResultSet resultForeignKeys) {

        List<LinkedHashMap<String, String>> rows = new LinkedList<LinkedHashMap<String, String>>();

        try {
            int columnsAmount = resultForeignKeys.getMetaData().getColumnCount();

            while (resultForeignKeys.next()) {

                if (isTableAllowed(resultForeignKeys.getString("pktable_name"))
                        && dataBaseSchema.getEntityByName(resultForeignKeys.getString("pktable_name")) != null) {
                    //          if(isTableAllowed(resultForeignKeys.getString("pktable_name")) && dataBaseSchema.getEntityByName(resultForeignKeys.getString("pktable_name")) != null) {

                    LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
                    for (int i = 1; i <= columnsAmount; i++) {
                        row.put(resultForeignKeys.getMetaData().getColumnName(i).toLowerCase(Locale.ENGLISH), resultForeignKeys.getString(i));
                    }
                    rows.add(row);
                }
            }
        } catch (SQLException e) {
            throw new RDBMSProviderRuntimeException(e);
        }
        return rows;
    }

    /**
     * MACRO EXECUTION BLOCK: BUILD GRAPH MODEL
     * Builds the graph model and the rules for the mapping with the source database schema through 2 micro execution blocks:
     * - Build Vertex Types
     * - Build Edge Types
     *
     * @param nameResolver the name resolver
     */

    public void buildGraphModel(NameResolver nameResolver) {

        statistics.startWork2Time = new Date();
        statistics.runningStepNumber = 2;


        /*
         *  Vertex-types building
         */

        buildVertexTypes(nameResolver);


        /*
         *  Edge-types building
         */

        buildEdgeTypes(nameResolver);

        statistics.runningStepNumber = -1;
    }

    /**
     * MICRO EXECUTION BLOCK: BUILD GRAPH MODEL - BUILD VERTEX TYPES
     * Builds the Vertex Types starting from the Entities in the source database schema.
     *
     * @param nameResolver the name Resolver
     */

    private void buildVertexTypes(NameResolver nameResolver) {


        int numberOfVertexType = dataBaseSchema.getEntities().size();
        statistics.totalNumberOfModelVertices = numberOfVertexType;
        int iteration = 1;
        for (Entity currentEntity : dataBaseSchema.getEntities()) {

            log.debug("Building '{}' vertex-type ({}/{})...", currentEntity.getName(), iteration, numberOfVertexType);

            // building correspondent vertex-type
            String currentVertexTypeName = nameResolver.resolveVertexName(currentEntity.getName());

            // fetch the vertex type from the graph model (empty vertex, only name defined), if does not exist create it.
            boolean alreadyPresentInGraphModel = true;
            VertexType currentVertexType = graphModel.getVertexTypeByName(currentVertexTypeName);
            if (currentVertexType == null) {
                currentVertexType = new VertexType(currentVertexTypeName);
                alreadyPresentInGraphModel = false;
            }

            // recognizing joint tables of dimension 2
            if (currentEntity.isAggregableJoinTable())
                currentVertexType.setIsFromJoinTable(true);
            else
                currentVertexType.setIsFromJoinTable(false);

            // adding attributes to vertex-type
            Map<String, String> attribute2property = new LinkedHashMap<>();   // map to maintain the mapping between the attributes of the current entity and the properties of the correspondent vertex type
            Map<String, String> property2attribute = new LinkedHashMap<>();   // map to maintain the mapping between the properties of the current vertex type and the attributes of the correspondent entity
            for (Attribute currentAttribute : currentEntity.getAttributes()) {
                String orientdbDataType = TypeMapperKt.mapType(currentAttribute.getDataType().toLowerCase(Locale.ENGLISH));
                ModelProperty currentProperty = new ModelProperty(nameResolver.resolveVertexProperty(currentAttribute.getName()),
                        currentAttribute.getOrdinalPosition(), currentAttribute.getDataType(),
                        currentEntity.getPrimaryKey().getInvolvedAttributes().contains(currentAttribute), currentVertexType);
                currentProperty.setOrientdbType(orientdbDataType);
                currentVertexType.getProperties().add(currentProperty);

                attribute2property.put(currentAttribute.getName(), currentProperty.getName());
                property2attribute.put(currentProperty.getName(), currentAttribute.getName());
            }

            // adding inherited attributes to vertex-type
            for (Attribute attribute : currentEntity.getInheritedAttributes()) {
                ModelProperty currentProperty = new ModelProperty(nameResolver.resolveVertexProperty(attribute.getName()),
                        attribute.getOrdinalPosition(), attribute.getDataType(),
                        currentEntity.getPrimaryKey().getInvolvedAttributes().contains(attribute), currentVertexType);
                currentVertexType.getInheritedProperties().add(currentProperty);
                // TODO: Adding inherited attributes and props to the maps?
            }

            // setting externalKey
            Set<String> externalKey = new LinkedHashSet<String>();
            for (ModelProperty currentProperty : currentVertexType.getAllProperties()) {
                // only attribute coming from the primary key are given
                if (currentProperty.isFromPrimaryKey()) {
                    externalKey.add(currentProperty.getName());
                }
            }
            currentVertexType.setExternalKey(externalKey);

            // adding parent vertex if the corresponding entity has a parent
            if (currentEntity.getParentEntity() != null) {
                ElementType currentParentElement = graphModel
                        .getVertexTypeByNameIgnoreCase(currentEntity.getParentEntity().getName());
                currentVertexType.setParentType(currentParentElement);
                currentVertexType.setInheritanceLevel(currentEntity.getInheritanceLevel());
            }

            // adding vertex to the graph model
            if (!alreadyPresentInGraphModel) {
                graphModel.getVerticesType().add(currentVertexType);
            }

            // rules updating
            EVClassMapper classMapper = new EVClassMapper(currentEntity, currentVertexType, attribute2property, property2attribute);
            upsertEVClassMappingRules(currentEntity, currentVertexType, classMapper);

            iteration++;
            log.debug("Vertex-type {} built.", currentVertexTypeName);
            statistics.builtModelVertexTypes++;
        }

        // sorting vertices type for inheritance level and then for name
        Collections.sort(graphModel.getVerticesType());
    }

    /**
     * MICRO EXECUTION BLOCK: BUILD GRAPH MODEL - BUILD EDGE TYPES
     * Builds the Edge Types starting from the Relationships in the source database schema.
     *
     * @param nameResolver the name resolver
     */

    private void buildEdgeTypes(NameResolver nameResolver) {


        int numberOfEdgeType = dataBaseSchema.getCanonicalRelationships().size();
        statistics.totalNumberOfModelEdges = numberOfEdgeType;
        String edgeType = null;
        int iteration = 1;

        if (numberOfEdgeType > 0) {

            // edges added through relationships (foreign keys of db)
            for (Entity currentEntity : dataBaseSchema.getEntities()) {

                for (CanonicalRelationship relationship : currentEntity.getOutCanonicalRelationships()) {

                    VertexType currentOutVertex = getVertexTypeByEntity(relationship.getForeignEntity());
                    VertexType currentInVertex = getVertexTypeByEntity(relationship.getParentEntity());

                    log.debug("Building edge-type from '{}' to '{}' ({}/{})...", currentOutVertex.getName(), currentInVertex.getName(), iteration, numberOfEdgeType);

                    if (currentOutVertex != null && currentInVertex != null) {

                        // check on the presence of the relationship in the map performed in order to avoid generating several edgeTypes for the same relationship.
                        // when the edge was built before from the migrationConfigDoc and the relationship was inserted with that edgeType in the map, the relationships
                        // mustn't be analyzed at point! CHANGE IT when you'll implement the pipeline
                        if (!relationship2edgeType.containsKey(relationship)) {

                            // relationships which represents inheritance between different entities don't generate new edge-types,
                            // thus new edge type is created iff the parent-table's name (of the relationship) does not coincide
                            // with the name of the parent entity of the current entity.
                            if (currentEntity.getParentEntity() == null || !currentEntity.getParentEntity().getName()
                                    .equals(relationship.getParentEntity().getName())) {

                                // if the class edge doesn't exists, it will be created
                                edgeType = nameResolver.resolveEdgeName(relationship);

                                EdgeType currentEdgeType = graphModel.getEdgeTypeByName(edgeType);
                                if (currentEdgeType == null) {
                                    currentEdgeType = new EdgeType(edgeType, null, currentInVertex);
                                    graphModel.getEdgesType().add(currentEdgeType);

                                    log.debug("Edge-type {} built.", currentEdgeType.getName());
                                    statistics.builtModelEdgeTypes++;
                                } else {
                                    // edge already present, the counter of relationships represented by the edge is incremented
                                    currentEdgeType.setNumberRelationshipsRepresented(currentEdgeType.getNumberRelationshipsRepresented() + 1);
                                }

                                // adding the edge to the two vertices
                                if (!currentOutVertex.getOutEdgesType().contains(currentEdgeType)) {
                                    currentOutVertex.getOutEdgesType().add(currentEdgeType);
                                }
                                if (!currentInVertex.getInEdgesType().contains(currentEdgeType)) {
                                    currentInVertex.getInEdgesType().add(currentEdgeType);
                                }

                                // rules updating
                                upsertRelationshipEdgeRules(relationship, currentEdgeType);
                            }
                        }
                    } else {
                        log.error("Error during graph model building phase: information loss, relationship missed. Edge-type not built.\n");
                    }

                    iteration++;
                }

                // building edges starting from inherited relationships

                for (CanonicalRelationship relationship : currentEntity.getInheritedOutCanonicalRelationships()) {
                    VertexType currentOutVertex = getVertexTypeByEntity(currentEntity);
                    VertexType currentInVertex = getVertexTypeByEntity(relationship.getParentEntity());

                    log.debug("Building edge-type from '{}' to '{}' ({}/{})...", currentOutVertex.getName(), currentInVertex.getName(),
                            iteration, numberOfEdgeType);

                    if (currentOutVertex != null && currentInVertex != null) {

                        EdgeType currentEdgeType = graphModel.getEdgeTypeByName(edgeType);

                        // adding the edge to the two vertices
                        currentOutVertex.getOutEdgesType().add(currentEdgeType);
                        currentInVertex.getInEdgesType().add(currentEdgeType);

                        log.debug("Edge-type built.");
                    } else {
                        log.error("Error during graph model building phase: information loss, relationship missed. Edge-type not built.\n");
                    }
                }
            }

            // Updating the total number of model edges with the actual number of built model edges since it was initialized with the number of relationships in the source db schema.
            // In fact if there are relationships representing hierarchy then the number of built edges is less than the number of relationships.
            statistics.totalNumberOfModelEdges = statistics.builtModelEdgeTypes;

        }
    }


    /**
     * MACRO EXECUTION BLOCK: PERFORM AGGREGATIONS
     * Performs aggregation strategies on the graph model through the following micro execution blocks:
     * - Many-To-Many Aggregation
     */

    public void performAggregations() {

        /*
         * Many-To-Many Aggregation
         */
        performMany2ManyAggregation();
    }

    /**
     * MICRO EXECUTION BLOCK: PERFORM AGGREGATIONS - MANY TO MANY AGGREGATION
     * Aggregates Many-To-Many Relationships represented by join tables of dimension == 2.
     */

    public void performMany2ManyAggregation() {

        Iterator<VertexType> it = graphModel.getVerticesType().iterator();

        log.debug("Join Table aggregation phase...");

        while (it.hasNext()) {
            VertexType currentVertexType = it.next();

            // if vertex is obtained from a join table of dimension 2,
            // then aggregation is performed
            if (currentVertexType.isFromJoinTable() && currentVertexType.getOutEdgesType().size() == 2) {

                // building new edge
                EdgeType currentOutEdge1 = currentVertexType.getOutEdgesType().get(0);
                EdgeType currentOutEdge2 = currentVertexType.getOutEdgesType().get(1);

                VertexType outVertexType;
                VertexType inVertexType;
                String direction = getEntityByVertexType(currentVertexType).getDirectionOfN2NRepresentedRelationship();
                if (direction.equals("direct")) {
                    outVertexType = currentOutEdge1.getInVertexType();
                    inVertexType = currentOutEdge2.getInVertexType();
                } else {
                    outVertexType = currentOutEdge2.getInVertexType();
                    inVertexType = currentOutEdge1.getInVertexType();
                }

                Entity joinTable = getEntityByVertexType(currentVertexType);
                String nameOfRelationship = joinTable.getNameOfN2NRepresentedRelationship();
                String edgeType;
                if (nameOfRelationship != null)
                    edgeType = nameOfRelationship;
                else
                    edgeType = currentVertexType.getName();

                EdgeType newAggregatorEdge = new EdgeType(edgeType, outVertexType, inVertexType);
                newAggregatorEdge.setIsAggregatorEdge(true);

                int position = 1;
                // adding to the edge all properties not belonging to the primary key
                for (ModelProperty currentProperty : currentVertexType.getProperties()) {

                    // if property does not belong to the primary key add it to the aggregator edge
                    if (!currentProperty.isFromPrimaryKey()) {
                        ModelProperty newProperty = new ModelProperty(currentProperty.getName(), position, currentProperty.getOriginalType(),
                                currentProperty.isFromPrimaryKey(), newAggregatorEdge);
                        newProperty.setOrientdbType(currentProperty.getOrientdbType());
                        if (currentProperty.isMandatory() != null)
                            newProperty.setMandatory(currentProperty.isMandatory());
                        if (currentProperty.isReadOnly() != null)
                            newProperty.setReadOnly(currentProperty.isReadOnly());
                        if (currentProperty.isNotNull() != null)
                            newProperty.setNotNull(currentProperty.isNotNull());
                        newAggregatorEdge.getProperties().add(newProperty);
                        position++;
                    }
                }

                // adding to the edge all properties belonging to the old edges
                for (ModelProperty currentProperty : currentOutEdge1.getProperties()) {
                    if (newAggregatorEdge.getPropertyByName(currentProperty.getName()) == null) {
                        ModelProperty newProperty = new ModelProperty(currentProperty.getName(), position, currentProperty.getOriginalType(),
                                currentProperty.isFromPrimaryKey(), newAggregatorEdge);
                        newProperty.setOrientdbType(currentProperty.getOrientdbType());
                        if (currentProperty.isMandatory() != null)
                            newProperty.setMandatory(currentProperty.isMandatory());
                        if (currentProperty.isReadOnly() != null)
                            newProperty.setReadOnly(currentProperty.isReadOnly());
                        if (currentProperty.isNotNull() != null)
                            newProperty.setNotNull(currentProperty.isNotNull());
                        newAggregatorEdge.getProperties().add(newProperty);
                        position++;
                    }
                }
                for (ModelProperty currentProperty : currentOutEdge2.getProperties()) {
                    if (newAggregatorEdge.getPropertyByName(currentProperty.getName()) == null) {
                        ModelProperty newProperty = new ModelProperty(currentProperty.getName(), position, currentProperty.getOriginalType(),
                                currentProperty.isFromPrimaryKey(), newAggregatorEdge);
                        newProperty.setOrientdbType(currentProperty.getOrientdbType());
                        if (currentProperty.isMandatory() != null)
                            newProperty.setMandatory(currentProperty.isMandatory());
                        if (currentProperty.isReadOnly() != null)
                            newProperty.setReadOnly(currentProperty.isReadOnly());
                        if (currentProperty.isNotNull() != null)
                            newProperty.setNotNull(currentProperty.isNotNull());
                        newAggregatorEdge.getProperties().add(newProperty);
                        position++;
                    }
                }

                // removing old edges from graph model and from vertices' "in-edges" collection
                currentOutEdge1.setNumberRelationshipsRepresented(currentOutEdge1.getNumberRelationshipsRepresented() - 1);
                currentOutEdge2.setNumberRelationshipsRepresented(currentOutEdge2.getNumberRelationshipsRepresented() - 1);

                if (currentOutEdge1.getNumberRelationshipsRepresented() == 0) {
                    graphModel.getEdgesType().remove(currentOutEdge1);
                    statistics.builtModelEdgeTypes--;
                    statistics.totalNumberOfModelEdges--;
                }
                if (currentOutEdge2.getNumberRelationshipsRepresented() == 0) {
                    graphModel.getEdgesType().remove(currentOutEdge2);
                    statistics.builtModelEdgeTypes--;
                    statistics.totalNumberOfModelEdges--;
                }
                if (direction.equals("direct")) {
                    outVertexType.getInEdgesType().remove(currentOutEdge1);
                    inVertexType.getInEdgesType().remove(currentOutEdge2);
                } else {
                    outVertexType.getInEdgesType().remove(currentOutEdge2);
                    inVertexType.getInEdgesType().remove(currentOutEdge1);
                }

                // adding entry to the map
                joinVertex2aggregatorEdges
                        .put(currentVertexType, new AggregatorEdge(outVertexType.getName(), inVertexType.getName(), newAggregatorEdge));

                // removing old vertex
                it.remove();
                statistics.builtModelVertexTypes--;
                statistics.totalNumberOfModelVertices--;

                // adding new edge to graph model
                graphModel.getEdgesType().add(newAggregatorEdge);
                statistics.builtModelEdgeTypes++;
                statistics.totalNumberOfModelEdges++;

                // adding new edge to the vertices' "in/out-edges" collections
                outVertexType.getOutEdgesType().add(newAggregatorEdge);
                inVertexType.getInEdgesType().add(newAggregatorEdge);
            }
        }

        log.debug("Aggregation performed.");
    }

    public DataBaseSchema getDataBaseSchema() {
        return dataBaseSchema;
    }

    public void setDataBaseSchema(DataBaseSchema dataBaseSchema) {
        dataBaseSchema = dataBaseSchema;
    }

    public Entity getEntityByVertexType(VertexType vertexType) {
        return getEntityByVertexType(vertexType, DEFAULT_CLASS_MAPPER_INDEX);
    }

    public Entity getEntityByVertexType(VertexType vertexType, int classMapperIndex) {
        return getEVClassMappersByVertex(vertexType).get(classMapperIndex).getEntity();
    }

    public Entity getEntityByNameIgnoreCase(String entityName) {
        for (Entity currentEntity : entity2EVClassMappers.keySet()) {
            if (entityName.equalsIgnoreCase(currentEntity.getName())) {
                return currentEntity;
            }
        }
        return null;
    }

    public Entity getEntityBySchemaPosition(int schemaPosition) {
        for (Entity currentEntity : entity2EVClassMappers.keySet()) {
            if (schemaPosition == currentEntity.getSchemaPosition()) {
                return currentEntity;
            }
        }
        return null;
    }

    public VertexType getVertexTypeByEntity(Entity entity) {
        return getVertexTypeByEntity(entity, DEFAULT_CLASS_MAPPER_INDEX);
    }

    public VertexType getVertexTypeByEntity(Entity entity, int classMapperIndex) {
        return getEVClassMappersByEntity(entity).get(classMapperIndex).getVertexType();
    }

    public VertexType getVertexTypeByEntityNameIgnoreCase(String entityName) {
        for (Entity currentEntity : entity2EVClassMappers.keySet()) {
            if (entityName.equalsIgnoreCase(currentEntity.getName())) {
                return entity2EVClassMappers.get(currentEntity).get(0).getVertexType();
            }
        }
        return null;
    }

    public VertexType getVertexTypeByEntityAndRelationship(Entity currentParentEntity, Relationship currentRelationship) {

        List<EVClassMapper> classMappers = getEVClassMappersByEntity(currentParentEntity);

        if (classMappers.size() == 1) {
            return getVertexTypeByEntity(currentParentEntity);
        } else {
            List<Attribute> toAttributes = currentRelationship.getToColumns();
            VertexType correspondentVertexType = null;

            for (EVClassMapper classMapper : classMappers) {
                boolean found = true;
                for (Attribute currAttribute : toAttributes) {
                    if (classMapper.getAttribute2property().get(currAttribute.getName()) == null) {
                        found = false;
                        break;
                    }
                }
                if (found) {
                    correspondentVertexType = classMapper.getVertexType();
                    break;
                }
            }
            return correspondentVertexType;
        }
    }

    public String getAttributeNameByVertexTypeAndProperty(VertexType vertexType, String propertyName) {

        String attributeName = null;

        for (EVClassMapper cm : getEVClassMappersByVertex(vertexType)) {
            attributeName = cm.getAttributeByProperty(propertyName);
            if (attributeName != null) {
                break;
            }
        }

        if (attributeName == null) {
            VertexType parentType = (VertexType) vertexType.getParentType();
            if (parentType != null) {
                return getAttributeNameByVertexTypeAndProperty(parentType, propertyName);
            }
        }

        return attributeName;
    }

    public String getPropertyNameByVertexTypeAndAttribute(VertexType vertexType, String attributeName) {

        List<EVClassMapper> classMappers = getEVClassMappersByVertex(vertexType);

        String propertyName = null;
        for (EVClassMapper currentClassMapper : classMappers) {
            propertyName = currentClassMapper.getPropertyByAttribute(attributeName);
            if (propertyName != null) {
                // the right class mapper was found and the right property name with it
                break;
            }
        }

        if (propertyName == null) {
            VertexType parentType = (VertexType) vertexType.getParentType();
            if (parentType != null) {
                return getPropertyNameByVertexTypeAndAttribute(parentType, attributeName);
            }
        }

        return propertyName;
    }

    public String getAttributeNameByEdgeTypeAndProperty(EdgeType edgeType, String propertyName) {

        String attributeName = null;

        for (EEClassMapper cm : getEEClassMappersByEdge(edgeType)) {
            attributeName = cm.getAttributeByProperty(propertyName);
            if (attributeName != null) {
                break;
            }
        }

        if (attributeName == null) {
            VertexType parentType = (VertexType) edgeType.getParentType();
            if (parentType != null) {
                return getAttributeNameByVertexTypeAndProperty(parentType, propertyName);
            }
        }

        return attributeName;
    }

    public String getPropertyNameByEntityAndAttribute(Entity entity, String attributeName) {

        List<EVClassMapper> classMappers = getEVClassMappersByEntity(entity);

        String propertyName = null;
        for (EVClassMapper currentClassMapper : classMappers) {
            propertyName = currentClassMapper.getPropertyByAttribute(attributeName);
            if (propertyName != null) {
                // the right class mapper was found and the right property name with it
                break;
            }
        }

        if (propertyName == null) {
            Entity parentEntity = (Entity) entity.getParentEntity();
            if (parentEntity != null) {
                return getPropertyNameByEntityAndAttribute(parentEntity, attributeName);
            }
        }

        return propertyName;
    }

    /**
     * It returns the vertex type mapped with the aggregator edge correspondent to the original join table.
     *
     * @param edgeType edgeType
     * @return vetex type
     */

    public VertexType getJoinVertexTypeByAggregatorEdge(String edgeType) {

        for (Map.Entry<VertexType, AggregatorEdge> entry : joinVertex2aggregatorEdges.entrySet()) {
            if (entry.getValue().getEdgeType().getName().equals(edgeType)) {
                VertexType joinVertexType = entry.getKey();
                return joinVertexType;
            }
        }
        return null;
    }

    public AggregatorEdge getAggregatorEdgeByJoinVertexTypeName(String vertexTypeName) {

        for (VertexType currentVertexType : joinVertex2aggregatorEdges.keySet()) {
            if (currentVertexType.getName().equals(vertexTypeName)) {
                return joinVertex2aggregatorEdges.get(currentVertexType);
            }
        }
        return null;
    }

    public VertexType getJoinVertexTypeByAggregatorEdgeName(String aggregatorEdgeName) {

        for (VertexType currentVertexType : joinVertex2aggregatorEdges.keySet()) {
            if (joinVertex2aggregatorEdges.get(currentVertexType).getEdgeType().getName().equals(aggregatorEdgeName)) {
                return currentVertexType;
            }
        }
        return null;
    }

    public AggregatorEdge getAggregatorEdgeByEdgeTypeName(String edgeTypeName) {

        for (AggregatorEdge currAggregatorEdge : joinVertex2aggregatorEdges.values()) {
            if (currAggregatorEdge.getEdgeType().getName().equals(edgeTypeName)) {
                return currAggregatorEdge;
            }
        }
        return null;
    }

    public List<Relationship> getRelationshipsByForeignAndParentTables(String currentForeignEntity, String currentParentEntity) {

        List<Relationship> relationships = new LinkedList<Relationship>();

        for (Relationship currentRelationship : dataBaseSchema.getCanonicalRelationships()) {
            if (currentRelationship.getForeignEntity().getName().equals(currentForeignEntity) && currentRelationship.getParentEntity()
                    .getName().equals(currentParentEntity)) {
                relationships.add(currentRelationship);
            }
        }
        return relationships;
    }

    public Map<Relationship, EdgeType> getRelationship2edgeType() {
        return relationship2edgeType;
    }


    public Map<EdgeType, LinkedList<Relationship>> getEdgeType2relationships() {
        return edgeType2relationships;
    }


    public Map<String, Integer> getEdgeTypeName2count() {
        return edgeTypeName2count;
    }


    public Map<VertexType, AggregatorEdge> getJoinVertex2aggregatorEdges() {
        return joinVertex2aggregatorEdges;
    }


    public List<String> getIncludedTables() {
        return includedTables;
    }

    public void setIncludedTables(List<String> includedTables) {
        includedTables = includedTables;
    }

    public List<String> getExcludedTables() {
        return excludedTables;
    }

    public void setExcludedTables(List<String> excludedTables) {
        excludedTables = excludedTables;
    }


    public boolean isTableAllowed(String tableName) {

        if (includedTables.size() > 0)
            return includedTables.contains(tableName);
        else if (excludedTables.size() > 0)
            return !excludedTables.contains(tableName);
        else
            return true;

    }

    public String toString() {

        String s = "\n\n\n------------------------------ MAPPER DESCRIPTION ------------------------------\n\n\n";
        s += "RULES\n\n";
        s += "- Class mappings:\n\n";
        for (List<EVClassMapper> classMappers : entity2EVClassMappers.values()) {
            for (EVClassMapper classMapper : classMappers) {
                s += classMapper.toString() + "\n";
            }
        }
        s += "\n\n- Relaionship2EdgeType Rules:\n\n";
        for (Relationship relationship : relationship2edgeType.keySet()) {
            s += relationship.getForeignEntity() + "2" + relationship.getParentEntity() + " --> " + relationship2edgeType
                    .get(relationship).getName() + "\n";
        }
        s += "\n\n- EdgeTypeName2Count Rules:\n\n";
        for (String edgeName : edgeTypeName2count.keySet()) {
            s += edgeName + " --> " + edgeTypeName2count.get(edgeName) + "\n";
        }
        s += "\n";

        return s;
    }

}
