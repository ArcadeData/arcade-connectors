package com.arcadeanalytics.provider.rdbms.model.dbschema;

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

import java.util.List;

/**
 * It represents a canonical relationship between two entities (foreign and parent entity)
 * based on the importing of a single primary key (composite or not) through a foreign key.
 *
 * @author Gabriele Ponzi
 */

public class CanonicalRelationship extends Relationship {

  private ForeignKey foreignKey;
  private PrimaryKey primaryKey;

  public CanonicalRelationship(Entity foreignEntity, Entity parentEntity) {
    this.foreignEntity = foreignEntity;
    this.parentEntity = parentEntity;
    this.direction = "direct";
  }

  public CanonicalRelationship(Entity foreignEntity, Entity parentEntity, ForeignKey foreignKey, PrimaryKey primaryKey) {
    this.foreignEntity = foreignEntity;
    this.parentEntity = parentEntity;
    this.foreignKey = foreignKey;
    this.primaryKey = primaryKey;
    this.direction = "direct";
  }

  @Override
  public List<Attribute> getFromColumns() {
    return this.foreignKey.getInvolvedAttributes();
  }

  @Override
  public List<Attribute> getToColumns() {
    return this.primaryKey.getInvolvedAttributes();
  }

  public ForeignKey getForeignKey() {
    return foreignKey;
  }

  public void setForeignKey(ForeignKey foreignKey) {
    this.foreignKey = foreignKey;
  }

  public PrimaryKey getPrimaryKey() {
    return primaryKey;
  }

  public void setPrimaryKey(PrimaryKey primaryKey) {
    this.primaryKey = primaryKey;
  }

  @Override
  public boolean equals(Object obj) {
    CanonicalRelationship that = (CanonicalRelationship) obj;
    if (this.foreignEntity.equals(that.getForeignEntity()) && this.parentEntity.equals(that.getParentEntity())) {
      if (this.foreignKey.equals(that.getForeignKey()) && this.primaryKey.equals(that.getPrimaryKey())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return (
      "CanonicalRelationship [foreignEntity=" +
      foreignEntity.getName() +
      ", parentEntity=" +
      parentEntity.getName() +
      ", Foreign key=" +
      this.foreignKey.toString() +
      ", Primary key=" +
      this.primaryKey.toString() +
      "]"
    );
  }
}
