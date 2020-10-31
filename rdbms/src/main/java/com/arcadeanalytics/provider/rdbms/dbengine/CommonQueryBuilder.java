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

import com.arcadeanalytics.provider.rdbms.model.dbschema.Attribute;
import com.arcadeanalytics.provider.rdbms.model.dbschema.Entity;
import com.arcadeanalytics.provider.rdbms.model.dbschema.HierarchicalBag;
import java.util.Iterator;
import java.util.List;

/**
 * Class implementing QueryBuilder interface: the implemented methods are usable with every DBMS.
 * This class is extended by DBMS-specific query builder implementations.
 *
 * @author Gabriele Ponzi
 */

public class CommonQueryBuilder implements QueryBuilder {
  protected String quote;

  public CommonQueryBuilder() {
    this.quote = "\"";
  }

  @Override
  public String countTableRecords(String currentTableName, String currentTableSchema) {
    String query;

    if (currentTableSchema != null) query = "select count(*) from " + currentTableSchema + "." + this.quote + currentTableName + this.quote; else query =
      "select count(*) from " + quote + currentTableName + this.quote;

    return query;
  }

  @Override
  public String getRecordById(Entity entity, String[] propertyOfKey, String[] valueOfKey) {
    String query;

    String entityName = entity.getName();
    String entitySchema = entity.getSchemaName();

    if (entitySchema != null) query = "select * from " + entitySchema + "." + this.quote + entityName + this.quote + " where "; else query =
      "select * from " + this.quote + entityName + this.quote + " where ";

    query += this.quote + propertyOfKey[0] + this.quote + " = '" + valueOfKey[0] + "'";

    if (propertyOfKey.length > 1) {
      for (int i = 1; i < propertyOfKey.length; i++) {
        query += " and " + this.quote + propertyOfKey[i] + this.quote + " = '" + valueOfKey[i] + "'";
      }
    }

    return query;
  }

  @Override
  public String getRecordsByEntity(Entity entity) {
    String query;

    String entityName = entity.getName();
    String entitySchema = entity.getSchemaName();

    if (entitySchema != null) query = "select * from " + entitySchema + "." + this.quote + entityName + this.quote; else query =
      "select * from " + this.quote + entityName + this.quote;

    return query;
  }

  @Override
  public String getRecordsFromMultipleEntities(List<Entity> mappedEntities, String[][] columns) {
    String query;

    Entity first = mappedEntities.get(0);
    if (first.getSchemaName() != null) query =
      "select * from " + first.getSchemaName() + "." + this.quote + first.getName() + this.quote + " as t0\n"; else query =
      "select * from " + this.quote + first.getName() + this.quote + " as t0\n";

    for (int i = 1; i < mappedEntities.size(); i++) {
      Entity currentEntity = mappedEntities.get(i);
      query += " full outer join " + currentEntity.getSchemaName() + "." + this.quote + currentEntity.getName() + this.quote + " as t" + i;
      query += " on t" + (i - 1) + "." + this.quote + columns[i - 1][0] + this.quote + " = t" + i + "." + this.quote + columns[i][0] + this.quote;

      for (int k = 1; k < columns[i].length; k++) {
        query += " and t" + (i - 1) + "." + this.quote + columns[i - 1][k] + this.quote + " = t" + i + "." + this.quote + columns[i][k] + this.quote;
      }

      query += "\n";
    }

    return query;
  }

  @Override
  public String getRecordsFromSingleTableByDiscriminatorValue(String discriminatorColumn, String currentDiscriminatorValue, Entity entity) {
    String query;

    String entityName = entity.getName();
    String entitySchema = entity.getSchemaName();

    if (entitySchema != null) query = "select * from " + entitySchema + "." + this.quote + entityName + this.quote; else query =
      "select * from " + this.quote + entityName + this.quote;

    query += " where " + this.quote + discriminatorColumn + this.quote + "='" + currentDiscriminatorValue + "'";

    return query;
  }

  @Override
  public String getEntityTypeFromSingleTable(String discriminatorColumn, Entity physicalEntity, String[] propertyOfKey, String[] valueOfKey) {
    String query;

    String physicalEntityName = physicalEntity.getName();
    String entitySchema = physicalEntity.getSchemaName();

    if (entitySchema != null) query =
      "select " + discriminatorColumn + " from " + entitySchema + "." + this.quote + physicalEntityName + this.quote + " where "; else query =
      "select " + discriminatorColumn + " from " + this.quote + physicalEntityName + this.quote + " where ";

    query += this.quote + propertyOfKey[0] + this.quote + " = '" + valueOfKey[0] + "'";

    if (propertyOfKey.length > 1) {
      for (int i = 1; i < propertyOfKey.length; i++) {
        query += " and " + this.quote + propertyOfKey[i] + this.quote + " = '" + valueOfKey[i] + "'";
      }
    }

    return query;
  }

  @Override
  public String buildAggregateTableFromHierarchicalBag(HierarchicalBag bag) {
    String query;

    Iterator<Entity> it = bag.getDepth2entities().get(0).iterator();
    Entity rootEntity = it.next();

    if (rootEntity.getSchemaName() != null) query =
      "select * from " + rootEntity.getSchemaName() + "." + this.quote + rootEntity.getName() + this.quote + " as t0\n"; else query =
      "select * from " + this.quote + rootEntity.getName() + this.quote + " as t0\n";

    String[] rootEntityPropertyOfKey = new String[rootEntity.getPrimaryKey().getInvolvedAttributes().size()]; // collects the attributes of the root-entity's primary key

    // filling the rootPropertyOfKey from the primary key of the rootEntity
    for (int j = 0; j < rootEntity.getPrimaryKey().getInvolvedAttributes().size(); j++) {
      rootEntityPropertyOfKey[j] = rootEntity.getPrimaryKey().getInvolvedAttributes().get(j).getName();
    }

    String[] currentEntityPropertyOfKey = new String[rootEntity.getPrimaryKey().getInvolvedAttributes().size()]; // collects the attributes of the current-entity's primary key

    Entity currentEntity;
    int thTable = 1;
    for (int i = 1; i < bag.getDepth2entities().size(); i++) {
      it = bag.getDepth2entities().get(i).iterator();

      while (it.hasNext()) {
        currentEntity = it.next();
        int index = 0;
        for (Attribute attribute : currentEntity.getPrimaryKey().getInvolvedAttributes()) {
          currentEntityPropertyOfKey[index] = attribute.getName();
          index++;
        }

        if (currentEntity.getSchemaName() != null) query +=
          "left join " + currentEntity.getSchemaName() + "." + this.quote + currentEntity.getName() + this.quote; else query +=
          "left join " + this.quote + currentEntity.getName() + this.quote;

        query +=
          " as t" +
          thTable +
          " on t0." +
          this.quote +
          rootEntityPropertyOfKey[0] +
          this.quote +
          " = t" +
          thTable +
          "." +
          this.quote +
          currentEntityPropertyOfKey[0] +
          this.quote;

        for (int k = 1; k < currentEntityPropertyOfKey.length; k++) {
          query +=
            " and " + this.quote + rootEntityPropertyOfKey[k] + this.quote + " = t" + thTable + "." + this.quote + currentEntityPropertyOfKey[0] + this.quote;
        }

        query += "\n";
        thTable++;
      }
    }

    return query;
  }
}
