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

import com.arcadeanalytics.provider.rdbms.model.DataSourceSchemaInfo;
import java.util.ArrayList;
import java.util.List;

/**
 * It represents the schema of a source DB with all its elements.
 *
 * @author Gabriele Ponzi
 */
public class DataBaseSchema implements DataSourceSchemaInfo {

  private int majorVersion;
  private int minorVersion;
  private int driverMajorVersion;
  private int driverMinorVersion;
  private String productName;
  private String productVersion;
  private List<Entity> entities;
  private List<CanonicalRelationship> canonicalRelationships;
  private List<LogicalRelationship> logicalRelationships;
  private List<HierarchicalBag> hierarchicalBags;

  public DataBaseSchema(
      int majorVersion,
      int minorVersion,
      int driverMajorVersion,
      int driverMinorVersion,
      String productName,
      String productVersion) {
    this.majorVersion = majorVersion;
    this.minorVersion = minorVersion;
    this.driverMajorVersion = driverMajorVersion;
    this.driverMinorVersion = driverMinorVersion;
    this.productName = productName;
    this.productVersion = productVersion;
    this.entities = new ArrayList<Entity>();
    this.canonicalRelationships = new ArrayList<CanonicalRelationship>();
    this.logicalRelationships = new ArrayList<LogicalRelationship>();
    this.hierarchicalBags = new ArrayList<HierarchicalBag>();
  }

  public DataBaseSchema() {
    this.entities = new ArrayList<Entity>();
    this.canonicalRelationships = new ArrayList<CanonicalRelationship>();
    this.logicalRelationships = new ArrayList<LogicalRelationship>();
    this.hierarchicalBags = new ArrayList<HierarchicalBag>();
  }

  public int getMajorVersion() {
    return majorVersion;
  }

  public void setMajorVersion(int majorVersion) {
    this.majorVersion = majorVersion;
  }

  public int getMinorVersion() {
    return minorVersion;
  }

  public void setMinorVersion(int minorVersion) {
    this.minorVersion = minorVersion;
  }

  public int getDriverMajorVersion() {
    return driverMajorVersion;
  }

  public void setDriverMajorVersion(int driverMajorVersion) {
    this.driverMajorVersion = driverMajorVersion;
  }

  public int getDriverMinorVersion() {
    return driverMinorVersion;
  }

  public void setDriverMinorVersion(int driverMinorVersion) {
    this.driverMinorVersion = driverMinorVersion;
  }

  public String getProductName() {
    return productName;
  }

  public void setProductName(String productName) {
    this.productName = productName;
  }

  public String getProductVersion() {
    return productVersion;
  }

  public void setProductVersion(String productVersion) {
    this.productVersion = productVersion;
  }

  public List<Entity> getEntities() {
    return entities;
  }

  public void setEntities(List<Entity> entitiess) {
    this.entities = entitiess;
  }

  public List<CanonicalRelationship> getCanonicalRelationships() {
    return canonicalRelationships;
  }

  public void setCanonicalRelationships(List<CanonicalRelationship> canonicalRelationships) {
    this.canonicalRelationships = canonicalRelationships;
  }

  public List<LogicalRelationship> getLogicalRelationships() {
    return logicalRelationships;
  }

  public void setLogicalRelationships(List<LogicalRelationship> logicalRelationships) {
    this.logicalRelationships = logicalRelationships;
  }

  public List<HierarchicalBag> getHierarchicalBags() {
    return hierarchicalBags;
  }

  public void setHierarchicalBags(List<HierarchicalBag> hierarchicalBags) {
    this.hierarchicalBags = hierarchicalBags;
  }

  public Entity getEntityByName(String entityName) {
    for (Entity currentEntity : this.entities) {
      if (currentEntity.getName().equals(entityName)) return currentEntity;
    }

    return null;
  }

  public Entity getEntityByNameIgnoreCase(String entityName) {
    for (Entity currentEntity : this.entities) {
      if (currentEntity.getName().equalsIgnoreCase(entityName)) return currentEntity;
    }

    return null;
  }

  public Entity getEntityByPosition(int position) {
    for (Entity currentEntity : this.entities) {
      if (currentEntity.getSchemaPosition() == position) return currentEntity;
    }

    return null;
  }

  public Relationship getRelationshipByInvolvedEntitiesAndAttributes(
      Entity currentForeignEntity,
      Entity currentParentEntity,
      List<String> fromColumns,
      List<String> toColumns) {
    for (Relationship currentRelationship : this.canonicalRelationships) {
      if (currentRelationship.getForeignEntity().getName().equals(currentForeignEntity.getName())
          && currentRelationship
              .getParentEntity()
              .getName()
              .equals(currentParentEntity.getName())) {
        if (sameAttributesInvolved(currentRelationship.getFromColumns(), fromColumns)
            && sameAttributesInvolved(currentRelationship.getToColumns(), toColumns)) {
          return currentRelationship;
        }
      }
    }
    return null;
  }

  /**
   * It checks if the attributes of a Key passed as parameter correspond to the string names in the
   * array columns. Order is not relevant.
   *
   * @param columns columns
   * @param columnsName names
   * @return true or false
   */
  private boolean sameAttributesInvolved(List<Attribute> columns, List<String> columnsName) {
    if (columns.size() != columnsName.size()) {
      return false;
    }

    for (String column : columnsName) {
      boolean present = false;
      for (Attribute attribute : columns) {
        if (attribute.getName().equals(column)) {
          present = true;
          break;
        }
      }
      if (!present) {
        return false;
      }
    }

    return true;
  }

  public String toString() {
    String s =
        "\n\n\n"
            + "------------------------------ DB SCHEMA DESCRIPTION"
            + " ------------------------------\n\n"
            + "\n"
            + "Product name: "
            + this.productName
            + "\tProduct version: "
            + this.productVersion
            + "\nMajor version: "
            + this.majorVersion
            + "\tMinor Version: "
            + this.minorVersion
            + "\nDriver major version: "
            + this.driverMajorVersion
            + "\tDriver minor version: "
            + this.driverMinorVersion
            + "\n\n\n";

    s +=
        "Number of Entities: "
            + this.entities.size()
            + ".\n"
            + "Number of Relationship: "
            + this.canonicalRelationships.size()
            + ".\n\n\n";

    for (Entity e : this.entities) s += e.toString();
    return s;
  }
}
