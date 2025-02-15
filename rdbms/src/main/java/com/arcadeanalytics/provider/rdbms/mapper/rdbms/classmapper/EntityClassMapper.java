package com.arcadeanalytics.provider.rdbms.mapper.rdbms.classmapper;

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

import com.arcadeanalytics.provider.rdbms.model.dbschema.Entity;
import java.util.Map;

/**
 * This abstract class is extended by all the classes responsible to map 2 classes of objects where
 * one of them is an Entity. Subclasses: - EVClassMapper: Entity --&gt; VertexType - EEClassMapper:
 * Entity --&gt; EdgeType
 *
 * @author Gabriele Ponzi
 */
public abstract class EntityClassMapper {

  protected Entity entity;
  protected Map<String, String> attribute2property;
  protected Map<String, String> property2attribute;

  public EntityClassMapper(
      Entity entity,
      Map<String, String> attribute2property,
      Map<String, String> property2attribute) {
    this.entity = entity;
    this.attribute2property = attribute2property;
    this.property2attribute = property2attribute;
  }

  public Entity getEntity() {
    return entity;
  }

  public void setEntity(Entity entity) {
    this.entity = entity;
  }

  public Map<String, String> getAttribute2property() {
    return attribute2property;
  }

  public void setAttribute2property(Map<String, String> attribute2property) {
    this.attribute2property = attribute2property;
  }

  public Map<String, String> getProperty2attribute() {
    return property2attribute;
  }

  public void setProperty2attribute(Map<String, String> property2attribute) {
    this.property2attribute = property2attribute;
  }

  public String getAttributeByProperty(String property) {
    return this.property2attribute.get(property);
  }

  public String getPropertyByAttribute(String attribute) {
    return this.attribute2property.get(attribute);
  }

  public boolean containsAttribute(String attributeName) {
    return this.attribute2property.containsKey(attributeName);
  }

  public boolean containsProperty(String propertyName) {
    return this.property2attribute.containsKey(propertyName);
  }
}
