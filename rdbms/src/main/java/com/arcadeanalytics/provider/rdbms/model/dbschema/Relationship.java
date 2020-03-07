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
 * It represents the relationship between two entities.
 *
 * @author Gabriele Ponzi
*/

public abstract class Relationship {

    protected Entity foreignEntity;        // Entity importing the key (starting entity)
    protected Entity parentEntity;            // Entity exporting the key (arrival entity)
    protected String direction;                    // represents the direction of the relationship

    public Entity getForeignEntity() {
        return this.foreignEntity;
    }


    public Entity getParentEntity() {
        return this.parentEntity;
    }


    public String getDirection() {
        return this.direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    public abstract List<Attribute> getFromColumns();

    public abstract List<Attribute> getToColumns();


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((foreignEntity == null) ? 0 : foreignEntity.hashCode());
        result = prime * result + ((parentEntity == null) ? 0 : parentEntity.hashCode());
        return result;
    }
}
