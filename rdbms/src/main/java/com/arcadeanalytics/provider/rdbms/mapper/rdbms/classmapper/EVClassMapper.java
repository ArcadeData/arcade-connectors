package com.arcadeanalytics.provider.rdbms.mapper.rdbms.classmapper;

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
import com.arcadeanalytics.provider.rdbms.model.graphmodel.VertexType;
import java.util.Map;

/**
 * This class is responsible to map 2 classes of objects: Entity and VertexType.
 * the following values are mapped:
 * - name of the entity -&gt; name of the vertex type
 * - each attribute of the entity -&gt; correspondent property of the vertex type
 * - each property of the vertex type -&gt; correspondent attribute of the entity
 *
 * @author Gabriele Ponzi
 */

public class EVClassMapper extends EntityClassMapper {

  private VertexType vertexType;

  public EVClassMapper(Entity entity, VertexType vertexType, Map<String, String> attribute2property, Map<String, String> property2attribute) {
    super(entity, attribute2property, property2attribute);
    this.vertexType = vertexType;
  }

  public VertexType getVertexType() {
    return vertexType;
  }

  public void setVertexType(VertexType vertexType) {
    this.vertexType = vertexType;
  }

  @Override
  public int hashCode() {
    int result = entity.hashCode();
    result = 31 * result + vertexType.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    EVClassMapper that = (EVClassMapper) o;

    if (!entity.equals(that.entity)) return false;
    if (!vertexType.equals(that.vertexType)) return false;
    if (!attribute2property.equals(that.attribute2property)) return false;
    return property2attribute.equals(that.property2attribute);
  }

  @Override
  public String toString() {
    String s = "{" + "Entity = " + entity.getName() + ", Vertex-Type = " + vertexType.getName() + ", attributes2properties: ";

    s += "[";
    for (String attribute : this.attribute2property.keySet()) {
      s += attribute + " --> " + attribute2property.get(attribute) + ", ";
    }
    s = s.substring(0, s.length() - 1);
    s += "]}";
    return s;
  }
}
