package com.arcadeanalytics.provider.rdbms.nameresolver;

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

import com.arcadeanalytics.provider.rdbms.model.dbschema.CanonicalRelationship;

/**
 * Interface that performs name transformations on the elements of the data source according to a
 * specific convention.
 *
 * @author Gabriele Ponzi
 */
public interface NameResolver {
  String resolveVertexName(String candidateName);

  String resolveVertexProperty(String candidateName);

  String resolveEdgeName(CanonicalRelationship relationship);
}
