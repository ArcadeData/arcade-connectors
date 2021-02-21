package com.arcadeanalytics.provider.rdbms.dataprovider;

/*-
 * #%L
 * Arcade Connectors
 * %%
 * Copyright (C) 2018 - 2021 ArcadeData
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
import com.arcadeanalytics.provider.Data;
import com.arcadeanalytics.provider.GraphData;
import com.arcadeanalytics.provider.Position;
import com.arcadeanalytics.provider.rdbms.mapper.rdbms.ER2GraphMapper;
import com.arcadeanalytics.provider.rdbms.model.dbschema.Attribute;
import com.arcadeanalytics.provider.rdbms.model.dbschema.Entity;
import com.arcadeanalytics.provider.rdbms.model.dbschema.PrimaryKey;
import com.arcadeanalytics.provider.rdbms.model.dbschema.Relationship;
import com.arcadeanalytics.provider.rdbms.model.graphmodel.ModelProperty;
import com.arcadeanalytics.provider.rdbms.model.graphmodel.VertexType;
import com.arcadeanalytics.provider.rdbms.persistence.util.QueryResult;
import com.arcadeanalytics.provider.rdbms.persistence.util.RelationshipQueryResult;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataFetcher {

    private final Logger log = LoggerFactory.getLogger(RDBMSDataProvider.class);
    private ER2GraphMapper mapper;

    public DataFetcher() {}

    public ER2GraphMapper getMapper() {
        return this.mapper;
    }

    public void setMapper(ER2GraphMapper mapper) {
        this.mapper = mapper;
    }

    public GraphData mapResultSet(
        QueryResult queryResult,
        Entity entity,
        List<RelationshipQueryResult> outCountResult,
        List<RelationshipQueryResult> inCountResult
    ) throws SQLException {
        // collections to build CytoGraph
        final Set<CytoData> cytoNodes = new LinkedHashSet<>();
        final Set<CytoData> cytoEdges = new LinkedHashSet<>();
        final Map<String, Map<String, Object>> nodeClasses = new LinkedHashMap<>();
        final Map<String, Map<String, Object>> edgeClasses = new LinkedHashMap<>();

        ResultSet scanningRecords = queryResult.getResult();

        String vertexClassName = this.mapper.getVertexTypeByEntity(entity).getName();

        // fetching properties from the graph model
        VertexType vertexType = mapper.getVertexTypeByEntity(entity);
        Set<ModelProperty> vertexTypeProperties = vertexType.getAllProperties();
        Map<String, Object> property2type = new LinkedHashMap<>();
        for (ModelProperty currProperty : vertexTypeProperties) {
            property2type.put(currProperty.getName(), currProperty.getOrientdbType());
        }
        nodeClasses.put(vertexClassName, property2type);

        Map<String, Integer> outRelationshipName2cardinality;
        Map<String, Integer> inRelationshipName2cardinality;

        int totalEdgeCount;

        // each record is imported as vertex in the orient graph
        while (scanningRecords.next()) {
            log.debug("current record:: {} ", scanningRecords);
            outRelationshipName2cardinality = new LinkedHashMap<>();
            inRelationshipName2cardinality = new LinkedHashMap<>();
            totalEdgeCount = 0;

            // move the cursor ahead fot the out relationships
            for (RelationshipQueryResult currentOutRelationshipsCount : outCountResult) {
                ResultSet currentOutRelationshipsCursor = currentOutRelationshipsCount.getResult();
                ResultSet currentCountRecord;
                if (currentOutRelationshipsCursor.next()) {
                    currentCountRecord = currentOutRelationshipsCursor;
                    int cardinality = currentCountRecord.getInt("connectionsCount");
                    totalEdgeCount += cardinality;
                    String relationshipName = currentOutRelationshipsCount.getRelationshipName();
                    if (outRelationshipName2cardinality.get(relationshipName) != null) {
                        cardinality += outRelationshipName2cardinality.get(relationshipName);
                    }
                    outRelationshipName2cardinality.put(relationshipName, cardinality);
                }
            }

            // move the cursor ahead fot the in relationships
            for (RelationshipQueryResult currentInRelationshipsCount : inCountResult) {
                ResultSet currentInRelationshipsCursor = currentInRelationshipsCount.getResult();
                ResultSet currentCountRecord;
                if (currentInRelationshipsCursor.next()) {
                    currentCountRecord = currentInRelationshipsCursor;
                    int cardinality = currentCountRecord.getInt("connectionsCount");
                    totalEdgeCount += cardinality;
                    String relationshipName = currentInRelationshipsCount.getRelationshipName();
                    if (inRelationshipName2cardinality.get(relationshipName) != null) {
                        cardinality += inRelationshipName2cardinality.get(relationshipName);
                    }
                    inRelationshipName2cardinality.put(currentInRelationshipsCount.getRelationshipName(), cardinality);
                }
            }

            // create a correspondent CytoNode
            CytoData currentConvertedData = toData(
                scanningRecords,
                entity,
                vertexClassName,
                totalEdgeCount,
                outRelationshipName2cardinality,
                inRelationshipName2cardinality
            );
            cytoNodes.add(currentConvertedData);
        }

        final GraphData data = new GraphData(nodeClasses, edgeClasses, cytoNodes, cytoEdges, false);
        return data;
    }

    private CytoData toData(
        ResultSet sourceRecord,
        Entity entity,
        String vertexClassName,
        int edgeCount,
        Map<String, Integer> outRelationshipName2cardinality,
        Map<String, Integer> inRelationshipName2cardinality
    ) throws SQLException {
        String id = getCytoIdFromPrimaryKey(sourceRecord, entity);

        HashMap<String, Object> targetRecord = new LinkedHashMap<String, Object>();
        for (Attribute attribute : entity.getAllAttributes()) {
            String propertyName = mapper.getPropertyNameByEntityAndAttribute(entity, attribute.getName());
            if (attribute.getDataType().equals("_text")) {
                // then we are handling an array field: we must deserialize it otherwise we'll get an exception during the map serialization performed by jackson
                String[] arrayValues = (String[]) sourceRecord.getArray(attribute.getName()).getArray();
                String stringifiedArray = Arrays.toString(arrayValues);
                targetRecord.put(propertyName, stringifiedArray);
            } else {
                targetRecord.put(propertyName, sourceRecord.getObject(attribute.getName()));
            }
        }
        targetRecord.put("@out", outRelationshipName2cardinality);
        targetRecord.put("@in", inRelationshipName2cardinality);
        targetRecord.put("@edgeCount", edgeCount);

        Data data = new Data(id, "", "", "", targetRecord);

        CytoData cyto = new CytoData(vertexClassName, "nodes", data, new Position(0.0, 0.0), "", "", "", "");
        return cyto;
    }

    @NotNull
    private String getCytoIdFromPrimaryKey(ResultSet sourceRecord, Entity entity) throws SQLException {
        PrimaryKey primaryKey = entity.getPrimaryKey();
        String id = entity.getSchemaPosition() + "_";

        for (Attribute attribute : primaryKey.getInvolvedAttributes()) {
            id += sourceRecord.getString(attribute.getName()) + "_";
        }
        id = id.substring(0, id.lastIndexOf("_"));
        return id;
    }

    public GraphData buildEdgesFromJoinResultAndRelationship(QueryResult queryResult, Relationship relationship, String direction) throws SQLException {
        ResultSet scanningRecords = queryResult.getResult();
        scanningRecords.beforeFirst();

        // collections to build CytoGraph
        final Set<CytoData> cytoNodes = new LinkedHashSet<CytoData>();
        final Set<CytoData> cytoEdges = new LinkedHashSet<CytoData>();
        final Map<String, Map<String, Object>> nodeClasses = new LinkedHashMap<String, Map<String, Object>>();
        final Map<String, Map<String, Object>> edgeClasses = new LinkedHashMap<String, Map<String, Object>>();

        // adding the edge class
        String edgeClassName = mapper.getRelationship2edgeType().get(relationship).getName();
        Map<String, Object> edgeClassInfo = new LinkedHashMap<String, Object>();
        edgeClasses.put(edgeClassName, edgeClassInfo);

        while (scanningRecords.next()) {
            String edgeId;
            String sourceId = null;
            String targetId = null;
            Entity foreignEntity = relationship.getForeignEntity();
            Entity parentEntity = relationship.getParentEntity();

            if (direction.equals("in")) {
                sourceId = this.getCytoIdFromPrimaryKey(scanningRecords, foreignEntity);
                targetId = parentEntity.getSchemaPosition() + "";
                for (Attribute currColumn : parentEntity.getPrimaryKey().getInvolvedAttributes()) {
                    targetId += "_" + scanningRecords.getString(currColumn.getName());
                }
            } else if (direction.equals("out")) {
                sourceId = foreignEntity.getSchemaPosition() + "";
                for (Attribute currColumn : foreignEntity.getPrimaryKey().getInvolvedAttributes()) {
                    sourceId += "_" + scanningRecords.getString(currColumn.getName());
                }
                targetId = this.getCytoIdFromPrimaryKey(scanningRecords, parentEntity);
            }
            edgeId = sourceId.replaceAll("_", "") + "_" + targetId.replaceAll("_", "");

            HashMap<String, Object> targetEdgeRecord = new LinkedHashMap<String, Object>();

            Data data = new Data(edgeId, "", sourceId, targetId, targetEdgeRecord);

            CytoData cyto = new CytoData(edgeClassName, "edges", data, new Position(0.0, 0.0), "", "", "", "");

            cytoEdges.add(cyto);
        }

        final GraphData graphData = new GraphData(nodeClasses, edgeClasses, cytoNodes, cytoEdges, false);
        return graphData;
    }

    public GraphData buildEdgesFromJoinTableRecords(
        QueryResult queryResult,
        String edgeClassName,
        Entity firstExternalEntity,
        Entity joinTable,
        Entity secondExternalEntity,
        String direction
    ) throws SQLException {
        ResultSet scanningRecords = queryResult.getResult();
        scanningRecords.beforeFirst();

        // collections to build CytoGraph
        final Set<CytoData> cytoNodes = new LinkedHashSet<CytoData>();
        final Set<CytoData> cytoEdges = new LinkedHashSet<CytoData>();
        final Map<String, Map<String, Object>> nodeClasses = new LinkedHashMap<String, Map<String, Object>>();
        final Map<String, Map<String, Object>> edgeClasses = new LinkedHashMap<String, Map<String, Object>>();

        // adding the edge class
        Map<String, Object> edgeClassInfo = new LinkedHashMap<String, Object>();
        edgeClasses.put(edgeClassName, edgeClassInfo);

        while (scanningRecords.next()) {
            // setting sourceId, targetId and edgeId
            String sourceId = null;
            String targetId = null;
            if (direction.equals("in")) {
                sourceId = this.getCytoIdFromPrimaryKey(scanningRecords, secondExternalEntity);
                targetId = this.getCytoIdFromPrimaryKey(scanningRecords, firstExternalEntity);
            } else if (direction.equals("out")) {
                sourceId = this.getCytoIdFromPrimaryKey(scanningRecords, firstExternalEntity);
                targetId = this.getCytoIdFromPrimaryKey(scanningRecords, secondExternalEntity);
            }
            String edgeId = this.getCytoIdFromPrimaryKey(scanningRecords, joinTable);

            // adding edge fields
            HashMap<String, Object> targetEdgeRecord = new LinkedHashMap<String, Object>();
            for (Attribute a : joinTable.getAllAttributes()) {
                if (!joinTable.getPrimaryKey().getInvolvedAttributes().contains(a)) {
                    String fieldName = a.getName();
                    Object fieldValue = scanningRecords.getObject(fieldName);
                    targetEdgeRecord.put(fieldName, fieldValue);
                }
            }

            Data data = new Data(edgeId, "", sourceId, targetId, targetEdgeRecord);

            CytoData cyto = new CytoData(edgeClassName, "edges", data, new Position(0.0, 0.0), "", "", "", "");

            cytoEdges.add(cyto);
        }

        final GraphData graphData = new GraphData(nodeClasses, edgeClasses, cytoNodes, cytoEdges, false);
        return graphData;
    }
}
