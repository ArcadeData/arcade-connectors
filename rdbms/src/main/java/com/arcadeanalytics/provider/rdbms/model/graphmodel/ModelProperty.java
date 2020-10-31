package com.arcadeanalytics.provider.rdbms.model.graphmodel;

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

/**
 * Class which holds all the attributes of a vertex property obtained from
 * the transformation of an attribute belonging to an entity of the source DB schema.
 *
 * @author Gabriele Ponzi
 */

public class ModelProperty {
  private String name;
  private int ordinalPosition;
  private String originalType;
  private String orientdbType;
  private boolean fromPrimaryKey;
  private ElementType belongingElementType;
  private boolean includedInMigration;

  // costraints
  private Boolean mandatory;
  private Boolean readOnly;
  private Boolean notNull;

  public ModelProperty(String name, int ordinalPosition, String originalType, boolean fromPrimaryKey, ElementType belongingElementType) {
    this.name = name;
    this.ordinalPosition = ordinalPosition;
    this.originalType = originalType;
    this.orientdbType = null;
    this.fromPrimaryKey = fromPrimaryKey;
    this.belongingElementType = belongingElementType;
    this.includedInMigration = true;
  }

  public ModelProperty(
    String name,
    int ordinalPosition,
    String originalType,
    String orientdbType,
    boolean fromPrimaryKey,
    ElementType belongingElementType,
    boolean mandatory,
    boolean readOnly,
    boolean notNull
  ) {
    this.name = name;
    this.ordinalPosition = ordinalPosition;
    this.originalType = originalType;
    this.orientdbType = orientdbType;
    this.fromPrimaryKey = fromPrimaryKey;
    this.belongingElementType = belongingElementType;
    this.mandatory = mandatory;
    this.readOnly = readOnly;
    this.notNull = notNull;
    this.includedInMigration = true;
  }

  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getOrdinalPosition() {
    return ordinalPosition;
  }

  public void setOrdinalPosition(int ordinalPosition) {
    this.ordinalPosition = ordinalPosition;
  }

  public String getOriginalType() {
    return this.originalType;
  }

  public void setOriginalType(String attributeType) {
    this.originalType = attributeType;
  }

  public String getOrientdbType() {
    return orientdbType;
  }

  public void setOrientdbType(String orientdbType) {
    this.orientdbType = orientdbType;
  }

  public boolean isFromPrimaryKey() {
    return this.fromPrimaryKey;
  }

  public void setFromPrimaryKey(boolean fromPrimaryKey) {
    this.fromPrimaryKey = fromPrimaryKey;
  }

  public ElementType getBelongingElementType() {
    return belongingElementType;
  }

  public void setBelongingElementType(ElementType belongingElementType) {
    this.belongingElementType = belongingElementType;
  }

  public Boolean isMandatory() {
    return this.mandatory;
  }

  public void setMandatory(Boolean mandatory) {
    this.mandatory = mandatory;
  }

  public Boolean isReadOnly() {
    return this.readOnly;
  }

  public void setReadOnly(Boolean readOnly) {
    this.readOnly = readOnly;
  }

  public Boolean isNotNull() {
    return this.notNull;
  }

  public void setNotNull(Boolean notNull) {
    this.notNull = notNull;
  }

  public boolean isIncludedInMigration() {
    return includedInMigration;
  }

  public void setIncludedInMigration(boolean includedInMigration) {
    this.includedInMigration = includedInMigration;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((originalType == null) ? 0 : originalType.hashCode());
    result = prime * result + (fromPrimaryKey ? 1231 : 1237);
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ordinalPosition;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    ModelProperty that = (ModelProperty) obj;
    if (!(this.name.equals(that.getName()) && this.ordinalPosition == that.getOrdinalPosition() && this.isFromPrimaryKey() == that.isFromPrimaryKey())) {
      return false;
    }
    if (this.originalType != null && that.originalType != null) {
      if (!this.originalType.equals(that.originalType)) {
        return false;
      }
    }
    if (this.orientdbType != null && that.orientdbType != null) {
      if (!this.orientdbType.equals(that.orientdbType)) {
        return false;
      }
    }
    return true;
  }

  public String toString() {
    String s = "";
    if (this.orientdbType != null) {
      s += this.ordinalPosition + ": " + this.name + " ( " + this.orientdbType + " )";
    } else {
      s += this.ordinalPosition + ": " + this.name;
    }
    return s;
  }
}
