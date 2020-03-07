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

import com.arcadeanalytics.provider.rdbms.model.dbschema.Entity;
import com.arcadeanalytics.provider.rdbms.model.dbschema.HierarchicalBag;

import java.util.List;

/**
 * Interface representing the query builder used by the DB Query Engine, hiding specific implementation for each DBMS.
 *
 * @author Gabriele Ponzi
*/

public interface QueryBuilder {

    String countTableRecords(String currentTableName, String currentTableSchema);

    String getRecordById(Entity entity, String[] propertyOfKey, String[] valueOfKey);

    String getRecordsByEntity(Entity entity);

    String getRecordsFromMultipleEntities(List<Entity> mappedEntities, String[][] columns);

    String getRecordsFromSingleTableByDiscriminatorValue(String discriminatorColumn, String currentDiscriminatorValue, Entity entity);

    String getEntityTypeFromSingleTable(String discriminatorColumn, Entity entity, String[] propertyOfKey, String[] valueOfKey);

    String buildAggregateTableFromHierarchicalBag(HierarchicalBag bag);
}
