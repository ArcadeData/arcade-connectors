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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * It represents the model of the destination GraphDB.
 *
 * @author Gabriele Ponzi
 */

public class GraphModel {

    private List<VertexType> verticesType;
    private List<EdgeType> edgesType;

    public GraphModel() {
        this.verticesType = new ArrayList<>();
        this.edgesType = new ArrayList<>();
    }

    public VertexType getVertexTypeByName(String name) {
        VertexType vertex = null;

        for (VertexType currentVertex : this.verticesType) {
            if (currentVertex.getName().equals(name)) {
                vertex = currentVertex;
                break;
            }
        }
        return vertex;
    }

    public VertexType getVertexTypeByNameIgnoreCase(String name) {
        VertexType vertex = null;

        for (VertexType currentVertex : this.verticesType) {
            if (currentVertex.getName().equalsIgnoreCase(name)) {
                vertex = currentVertex;
                break;
            }
        }
        return vertex;
    }

    public List<VertexType> getVerticesType() {
        return this.verticesType;
    }

    public List<EdgeType> getEdgesType() {
        return this.edgesType;
    }

    public EdgeType getEdgeTypeByName(String name) {
        for (EdgeType currentEdgetype : this.edgesType) {
            if (currentEdgetype.getName().equals(name)) {
                return currentEdgetype;
            }
        }
        return null;
    }

    public EdgeType getEdgeTypeByNameIgnoreCase(String name) {
        for (EdgeType currentEdgetype : this.edgesType) {
            if (currentEdgetype.getName().equalsIgnoreCase(name)) {
                return currentEdgetype;
            }
        }
        return null;
    }

    public boolean removeVertexTypeByName(String vertexName) {
        Iterator<VertexType> iterator = this.verticesType.iterator();

        while (iterator.hasNext()) {
            VertexType currVertexType = iterator.next();
            if (currVertexType.getName().equals(vertexName)) {
                // removing references from the in edges
                for (EdgeType currInEdgeType : currVertexType.getInEdgesType()) {
                    currInEdgeType.setInVertexType(null);
                }

                // removing the vertex
                iterator.remove();
                return true;
            }
        }
        return false;
    }

    public boolean removeEdgeTypeByName(String vertexName) {
        Iterator<EdgeType> iterator = this.edgesType.iterator();

        while (iterator.hasNext()) {
            EdgeType currEdgeType = iterator.next();
            if (currEdgeType.getName().equals(vertexName)) {
                iterator.remove();
                return true;
            }
        }
        return false;
    }

    public String toString() {
        String s = "\n\n\n------------------------------ MODEL GRAPH DESCRIPTION ------------------------------\n\n\n";

        s += "Number of Vertex-type: " + this.verticesType.size() + ".\nNumber of Edge-type: " + this.edgesType.size() + ".\n\n";

        // info about vertices
        s += "Vertex-type:\n\n";
        for (VertexType v : this.verticesType) s += v.toString() + "\n\n";

        s += "\n\n";

        // info about edges
        s += "Edge-type:\n\n";
        for (EdgeType e : this.edgesType) s += e.toString() + "\n";

        s += "\n\n";

        // graph structure
        s += "Graph structure:\n\n";
        for (VertexType v : this.verticesType) {
            for (EdgeType e : v.getOutEdgesType()) s += v.getName() + " -----------[" + e.getName() + "]-----------> " + e.getInVertexType().getName() + "\n";
        }

        return s;
    }
}
