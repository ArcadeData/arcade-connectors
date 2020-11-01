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

/**
 * It represents an attribute of an entity.
 *
 * @author Gabriele Ponzi
 */

public class Attribute implements Comparable<Attribute> {

  private final String name;
  private final String dataType;
  private final Entity belongingEntity;
  private int ordinalPosition;

  public Attribute(String name, int ordinalPosition, String dataType, Entity belongingEntity) {
    this.name = name;
    this.ordinalPosition = ordinalPosition;
    this.dataType = dataType;
    this.belongingEntity = belongingEntity;
  }

  public String getName() {
    return this.name;
  }

  public int getOrdinalPosition() {
    return this.ordinalPosition;
  }

  //FIXME: why the hell it should be updated after creation?
  public void setOrdinalPosition(int ordinalPosition) {
    this.ordinalPosition = ordinalPosition;
  }

  public String getDataType() {
    return this.dataType;
  }

  public Entity getBelongingEntity() {
    return this.belongingEntity;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((dataType == null) ? 0 : dataType.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    return result;
  }

  public boolean equals(Object o) {
    Attribute that = (Attribute) o;
    if (this.name.equals(that.getName()) && this.dataType.equals(that.getDataType())) {
      return true;
    } else return false;
  }

  @Override
  public int compareTo(Attribute attributeToCompare) {
    if (this.ordinalPosition > attributeToCompare.getOrdinalPosition()) return 0; else if (
      this.ordinalPosition < attributeToCompare.getOrdinalPosition()
    ) return -1; else return 1;
  }

  public String toString() {
    String s = "";
    s += this.ordinalPosition + ": " + this.name + " ( " + this.dataType + " )";
    return s;
  }
}
