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
 * It represents a generic key of the source DB. It's extended from PrimaryKey
 * and ForeignKey that differ each other only by their usage.
 *
 * @author Gabriele Ponzi
 */

public class Key {

    protected final Entity belongingEntity;
    protected final List<Attribute> involvedAttributes;

    public Key(Entity belongingEntity, List<Attribute> involvedAttributes) {
        this.belongingEntity = belongingEntity;
        this.involvedAttributes = involvedAttributes;
    }

    public Entity getBelongingEntity() {
        return this.belongingEntity;
    }

    public List<Attribute> getInvolvedAttributes() {
        return this.involvedAttributes;
    }

    public void addAttribute(Attribute attribute) {
        this.involvedAttributes.add(attribute);
    }

    public boolean removeAttribute(Attribute toRemove) {
        return this.involvedAttributes.remove(toRemove);
    }

    public Attribute getAttributeByName(String name) {
        Attribute toReturn = null;

        for (Attribute a : this.involvedAttributes) {
            if (a.getName().equals(name)) {
                toReturn = a;
                break;
            }
        }
        return toReturn;
    }

    public Attribute getAttributeByNameIgnoreCase(String name) {
        Attribute toReturn = null;

        for (Attribute a : this.involvedAttributes) {
            if (a.getName().equalsIgnoreCase(name)) {
                toReturn = a;
                break;
            }
        }
        return toReturn;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((belongingEntity == null) ? 0 : belongingEntity.getName().hashCode());
        result = prime * result + ((involvedAttributes == null) ? 0 : involvedAttributes.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        Key that = (Key) obj;

        if (this.belongingEntity.getName().equals(that.belongingEntity.getName())) {
            if (this.involvedAttributes.equals(that.getInvolvedAttributes())) {
                return true;
            }
        }

        return false;
    }

    public String toString() {
        String s = "[";
        for (Attribute attribute : this.involvedAttributes) {
            s += attribute.getName() + ",";
        }
        s = s.substring(0, s.length() - 1);
        s += "]";

        return s;
    }
}
