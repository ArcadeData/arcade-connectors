package com.arcadeanalytics.provider.rdbms.mapper.rdbms;

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

import com.arcadeanalytics.provider.rdbms.model.graphmodel.EdgeType;

/**
 * @author Gabriele Ponzi
 */

public class AggregatorEdge {

  private final String outVertexClassName;
  private final String inVertexClassName;
  private final EdgeType edgeType;

  public AggregatorEdge(String outVertexClassName, String inVertexClassName, EdgeType edgeType) {
    this.outVertexClassName = outVertexClassName;
    this.inVertexClassName = inVertexClassName;
    this.edgeType = edgeType;
  }

  public String getOutVertexClassName() {
    return outVertexClassName;
  }

  public String getInVertexClassName() {
    return inVertexClassName;
  }

  public EdgeType getEdgeType() {
    return edgeType;
  }
}
