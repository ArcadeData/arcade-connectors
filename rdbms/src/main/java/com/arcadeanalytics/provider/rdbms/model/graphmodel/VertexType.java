package com.arcadeanalytics.provider.rdbms.model.graphmodel;

/*-
 * #%L
 * Arcade Data
 * %%
 * Copyright (C) 2018 - 2019 ArcadeAnalytics
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

import com.tinkerpop.blueprints.Direction;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * It represents an Orient class of a specific type that extends the Orient Vertex Class.
 * It's a simple vertex-type in the graph model.
 *
 * @author Gabriele Ponzi
*/

public class VertexType extends ElementType {

    private List<EdgeType> inEdgesType;
    private List<EdgeType> outEdgesType;
    private boolean isFromJoinTable;
    private Set<String> externalKey;
    private boolean analyzedInLastMigration;

    public VertexType(String vertexType) {
        super(vertexType);
        this.inEdgesType = new ArrayList<EdgeType>();
        this.outEdgesType = new ArrayList<EdgeType>();
        this.externalKey = new LinkedHashSet<String>();
        this.analyzedInLastMigration = false;
    }

    public List<EdgeType> getInEdgesType() {
        return this.inEdgesType;
    }

    public void setInEdgesType(List<EdgeType> inEdgesType) {
        this.inEdgesType = inEdgesType;
    }

    public List<EdgeType> getOutEdgesType() {
        return this.outEdgesType;
    }

    public void setOutEdgesType(List<EdgeType> outEdgesType) {
        this.outEdgesType = outEdgesType;
    }

    public Set<String> getExternalKey() {
        return this.externalKey;
    }

    public void setExternalKey(Set<String> externalKey) {
        this.externalKey = externalKey;
    }

    public boolean isAnalyzedInLastMigration() {
        return this.analyzedInLastMigration;
    }

    public void setAnalyzedInLastMigration(boolean analyzedInLastMigration) {
        this.analyzedInLastMigration = analyzedInLastMigration;
    }

    public EdgeType getEdgeByName(String edgeName) {

        for (EdgeType currentEdgeType : this.inEdgesType) {
            if (currentEdgeType.getName().equals(edgeName))
                return currentEdgeType;
        }

        for (EdgeType currentEdgeType : this.outEdgesType) {
            if (currentEdgeType.getName().equals(edgeName))
                return currentEdgeType;
        }

        return null;

    }

    public EdgeType getEdgeByName(String name, Direction direction) {

        if (direction.equals(Direction.IN)) {
            for (EdgeType currentEdgeType : this.inEdgesType) {
                if (currentEdgeType.getName().equals(name))
                    return currentEdgeType;
            }
        } else if (direction.equals(Direction.OUT)) {
            for (EdgeType currentEdgeType : this.outEdgesType) {
                if (currentEdgeType.getName().equals(name))
                    return currentEdgeType;
            }
        } else if (direction.equals(Direction.BOTH)) {
            return this.getEdgeByName(name);
        }

        return null;
    }

    public boolean isFromJoinTable() {
        return this.isFromJoinTable;
    }

    public void setFromJoinTable(boolean fromJoinTable) {
        isFromJoinTable = fromJoinTable;
    }

    public void setIsFromJoinTable(boolean isFromJoinTable) {
        this.isFromJoinTable = isFromJoinTable;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((super.name == null) ? 0 : super.name.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {

        VertexType that = (VertexType) obj;

        // check on type and many-to-many variables
        if (!(super.name.equals(that.getName()) && this.isFromJoinTable == that.isFromJoinTable()))
            return false;

        // check on properties
        if (!(this.properties.equals(that.getProperties())))
            return false;

        // in&out edges
        if (!(this.inEdgesType.equals(that.getInEdgesType()) && this.outEdgesType.equals(that.getOutEdgesType())))
            return false;

        return true;
    }

    public String toString() {
        String s =
                "Vertex-type [type = " + super.name + ", # attributes = " + this.properties.size() + ", # inEdges: " + this.inEdgesType
                        .size() + ", # outEdges: " + this.outEdgesType.size() + "]\nAttributes:\n";

        for (ModelProperty currentProperty : this.properties) {
            s += currentProperty.getOrdinalPosition() + ": " + currentProperty.getName() + " --> " + currentProperty.toString();

            if (currentProperty.isFromPrimaryKey())
                s += "(from PK)";

            s += "\t";
        }
        return s;
    }

}
