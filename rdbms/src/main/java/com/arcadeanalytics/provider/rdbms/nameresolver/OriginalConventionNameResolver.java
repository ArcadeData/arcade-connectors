package com.arcadeanalytics.provider.rdbms.nameresolver;

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

import com.arcadeanalytics.provider.rdbms.model.dbschema.CanonicalRelationship;

/**
 * Implementation of NameResolver that maintains the original name convention.
 *
 * @author Gabriele Ponzi
 */

public class OriginalConventionNameResolver implements NameResolver {

  @Override
  public String resolveVertexName(String candidateName) {
    candidateName = candidateName.replace(" ", "_");
    return candidateName;
  }

  @Override
  public String resolveVertexProperty(String candidateName) {
    candidateName = candidateName.replace(" ", "_");
    return candidateName;
  }

  @Override
  public String resolveEdgeName(CanonicalRelationship relationship) {
    String finalName;

    // Foreign Key composed of 1 attribute
    if (relationship.getFromColumns().size() == 1) {
      String columnName = relationship.getFromColumns().get(0).getName();
      columnName = columnName.replace("_id", "");
      columnName = columnName.replace("_ID", "");
      columnName = columnName.replace("_oid", "");
      columnName = columnName.replace("_OID", "");
      columnName = columnName.replace("_eid", "");
      columnName = columnName.replace("_EID", "");

      // manipulating name (Java Convention)
      finalName = "has_" + columnName;
    }
    // Foreign Key composed of multiple attribute
    else {
      finalName = relationship.getForeignEntity().getName() + "2" + relationship.getParentEntity().getName();
    }

    return finalName;
  }
}
