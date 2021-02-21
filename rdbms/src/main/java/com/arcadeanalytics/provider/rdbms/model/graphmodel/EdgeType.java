package com.arcadeanalytics.provider.rdbms.model.graphmodel;

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

/**
 * It represents an Orient class of a specific type that extends the Orient Edge Class.
 * It's a simple edge-type in the graph model.
 *
 * @author Gabriele Ponzi
 */

public class EdgeType extends ElementType {

    private VertexType inVertexType;
    private VertexType outVertexType;
    private int numberRelationshipsRepresented; // the number of relationships represented by the edge
    private boolean isSplittingEdge;
    private boolean isAggregatorEdge;

    public EdgeType(String edgeType) {
        super(edgeType);
        this.numberRelationshipsRepresented = 1;
        this.isSplittingEdge = false;
    }

    public EdgeType(String edgeType, VertexType outVertexType, VertexType inVertexType) {
        super(edgeType);
        this.outVertexType = outVertexType;
        this.inVertexType = inVertexType;
        numberRelationshipsRepresented = 1;
        this.isSplittingEdge = false;
        this.isAggregatorEdge = false;
    }

    public EdgeType(
        String edgeType,
        VertexType outVertexType,
        VertexType inVertexType,
        int numberRelationshipsRepresented,
        boolean isSplittingEdge,
        boolean isAggregatorEdge
    ) {
        super(edgeType);
        this.outVertexType = outVertexType;
        this.inVertexType = inVertexType;
        this.numberRelationshipsRepresented = numberRelationshipsRepresented;
        this.isSplittingEdge = isSplittingEdge;
        this.isAggregatorEdge = isAggregatorEdge;
    }

    public VertexType getInVertexType() {
        return this.inVertexType;
    }

    public void setInVertexType(VertexType inVertexType) {
        this.inVertexType = inVertexType;
    }

    public VertexType getOutVertexType() {
        return outVertexType;
    }

    public void setOutVertexType(VertexType outVertexType) {
        this.outVertexType = outVertexType;
    }

    public int getNumberRelationshipsRepresented() {
        return this.numberRelationshipsRepresented;
    }

    public void setNumberRelationshipsRepresented(int numberRelationshipsRepresented) {
        this.numberRelationshipsRepresented = numberRelationshipsRepresented;
    }

    public boolean isSplittingEdge() {
        return this.isSplittingEdge;
    }

    public void setSplittingEdge(boolean splittingEdge) {
        isSplittingEdge = splittingEdge;
    }

    public boolean isAggregatorEdge() {
        return isAggregatorEdge;
    }

    public void setIsAggregatorEdge(boolean aggregatorEdge) {
        isAggregatorEdge = aggregatorEdge;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((super.name == null) ? 0 : super.name.hashCode());
        result = prime * result + ((inVertexType == null) ? 0 : inVertexType.hashCode());
        result = prime * result + ((outVertexType == null) ? 0 : outVertexType.hashCode());
        result = prime * result + ((properties == null) ? 0 : properties.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        EdgeType that = (EdgeType) obj;

        // check on type and in/out vertex
        if (!(super.name.equals(that.getName()) && this.inVertexType.getName().equals(that.getInVertexType().getName()))) return false;

        // check on properties
        for (ModelProperty currentProperty : this.properties) {
            if (!(that.getProperties().contains(currentProperty))) return false;
        }

        return true;
    }

    public String toString() {
        String s = "";

        if (this.outVertexType != null && this.inVertexType != null) s =
            "Edge-type [type = " +
            super.name +
            ", out-vertex-type = " +
            this.getOutVertexType().getName() +
            ", in-vertex-type = " +
            this.getInVertexType().getName() +
            " ]"; else s = "Edge-type [type = " + super.name + " ]";

        if (this.properties.size() > 0) {
            s += "\nEdge's properties (" + this.properties.size() + "):\n";
            for (ModelProperty property : this.properties) {
                s += property.getName() + " --> " + property.toString() + "\n";
            }
        }
        s += "\n";
        return s;
    }
}
