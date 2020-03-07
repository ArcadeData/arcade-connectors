package com.arcadeanalytics.provider.rdbms.model.dbschema;

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

import java.util.List;

/**
 * It represents a logical relationship between two entities, as to say 2 set of columns belonging
 * to 2 different entities on which is possible to perform a join operation.
 *
 * @author Gabriele Ponzi
*/

public class LogicalRelationship extends Relationship {

    private List<Attribute> fromColumns;
    private List<Attribute> toColumns;

    public LogicalRelationship(Entity foreignEntity, Entity parentEntity) {
        this.foreignEntity = foreignEntity;
        this.parentEntity = parentEntity;
        this.direction = "direct";
    }

    public LogicalRelationship(Entity foreignEntity, Entity parentEntity, List<Attribute> fromColumns,
                               List<Attribute> toColumns) {
        this.foreignEntity = foreignEntity;
        this.parentEntity = parentEntity;
        this.fromColumns = fromColumns;
        this.toColumns = toColumns;
        this.direction = "direct";
    }

    @Override
    public List<Attribute> getFromColumns() {
        return fromColumns;
    }

    public void setFromColumns(List<Attribute> columns) {
        this.fromColumns = columns;
    }

    @Override
    public List<Attribute> getToColumns() {
        return this.toColumns;
    }

    public void setToColumns(List<Attribute> columns) {
        this.toColumns = columns;
    }

    @Override
    public boolean equals(Object obj) {
        CanonicalRelationship that = (CanonicalRelationship) obj;
        if (this.foreignEntity.equals(that.getForeignEntity()) && this.parentEntity.equals(that.getParentEntity())) {
            if (this.fromColumns.equals(that.getFromColumns()) && this.toColumns.equals(that.getToColumns())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {

        String fromColumns = "[";
        for (Attribute attribute : this.fromColumns) {
            fromColumns += attribute.getName() + ",";
        }
        fromColumns = fromColumns.substring(0, fromColumns.length() - 1);
        fromColumns += "]";

        String toColumns = "[";
        for (Attribute attribute : this.toColumns) {
            toColumns += attribute.getName() + ",";
        }
        toColumns = toColumns.substring(0, toColumns.length() - 1);
        toColumns += "]";

        return "LogicalRelationship [foreignEntity=" + foreignEntity.getName() + ", parentEntity=" + parentEntity.getName()
                + ", From Columns=" + fromColumns + ", To Columns=" + toColumns + " ]";
    }
}
