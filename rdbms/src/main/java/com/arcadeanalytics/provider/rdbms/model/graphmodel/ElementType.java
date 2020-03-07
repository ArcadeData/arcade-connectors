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

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * It represents an Orient class. It could be a Vertex-Type or an Edge-Type in
 * the graph model.
 *
 * @author Gabriele Ponzi
*/

public class ElementType implements Comparable<ElementType> {

    protected String name;
    protected List<ModelProperty> properties;
    protected List<ModelProperty> inheritedProperties;
    protected Set<ModelProperty> allProperties;
    protected ElementType parentType;
    protected int inheritanceLevel;

    public ElementType(String type) {
        this.name = type;
        this.properties = new LinkedList<>();
        this.inheritedProperties = new LinkedList<>();
        this.allProperties = null;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String type) {
        this.name = type;
    }

    public List<ModelProperty> getProperties() {
        return this.properties;
    }

    public void setProperties(List<ModelProperty> properties) {
        this.properties = properties;
    }

    public List<ModelProperty> getInheritedProperties() {
        return this.inheritedProperties;
    }

    public void setInheritedProperties(List<ModelProperty> inheritedProperties) {
        this.inheritedProperties = inheritedProperties;
    }

    public ElementType getParentType() {
        return this.parentType;
    }

    public void setParentType(ElementType parentType) {
        this.parentType = parentType;
    }

    public int getInheritanceLevel() {
        return this.inheritanceLevel;
    }

    public void setInheritanceLevel(int inheritanceLevel) {
        this.inheritanceLevel = inheritanceLevel;
    }

    public ModelProperty getPropertyByOrdinalPosition(int position) {
        for (ModelProperty property : this.properties) {
            if (property.getOrdinalPosition() == position) {
                return property;
            }
        }
        return null;
    }

    public void removePropertyByName(String toRemove) {
        Iterator<ModelProperty> it = this.properties.iterator();
        ModelProperty currentProperty = null;

        while (it.hasNext()) {
            currentProperty = it.next();
            if (currentProperty.getName().equals(toRemove))
                it.remove();
        }
    }

    public ModelProperty getPropertyByName(String name) {
        for (ModelProperty property : this.properties) {
            if (property.getName().equals(name)) {
                return property;
            }
        }
        return null;
    }

    public ModelProperty getInheritedPropertyByName(String name) {
        for (ModelProperty property : this.inheritedProperties) {
            if (property.getName().equals(name)) {
                return property;
            }
        }
        return null;
    }

    public ModelProperty getPropertyByNameAmongAll(String name) {
        for (ModelProperty property : this.getAllProperties()) {
            if (property.getName().equals(name)) {
                return property;
            }
        }
        return null;
    }

    // Returns properties and inherited properties
    public Set<ModelProperty> getAllProperties() {

        if (allProperties == null) {
            allProperties = new LinkedHashSet<ModelProperty>();
            allProperties.addAll(this.inheritedProperties);
            allProperties.addAll(this.properties);
        }

        return allProperties;
    }

    @Override
    public int compareTo(ElementType toCompare) {

        if (this.inheritanceLevel > toCompare.getInheritanceLevel())
            return 1;
        else if (this.inheritanceLevel < toCompare.getInheritanceLevel())
            return -1;
        else
            return this.name.compareTo(toCompare.getName());

    }

}
