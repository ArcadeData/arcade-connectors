package com.arcadeanalytics.provider.rdbms.dbengine;

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

import com.arcadeanalytics.provider.rdbms.model.dbschema.Attribute;
import com.arcadeanalytics.provider.rdbms.model.dbschema.Entity;
import java.util.List;

/**
 * Query Builder for PostgreSQL DBMS. It extends the CommonQueryBuilder class and overrides only the
 * needed methods.
 *
 * @author Gabriele Ponzi
 */
public class PostgreSQLQueryBuilder extends CommonQueryBuilder {

  public String buildGeospatialQuery(Entity entity, List<String> geospatialTypes) {
    String query = "select ";

    for (Attribute currentAttribute : entity.getAllAttributes()) {
      if (this.isGeospatial(geospatialTypes, currentAttribute.getDataType()))
        query +=
            "ST_AsText("
                + quote
                + currentAttribute.getName()
                + quote
                + ") as "
                + currentAttribute.getName()
                + ",";
      else query += quote + currentAttribute.getName() + quote + ",";
    }

    query = query.substring(0, query.length() - 1);

    String entitySchema = entity.getSchemaName();

    if (entitySchema != null)
      query += " from " + entitySchema + "." + quote + entity.getName() + quote;
    else query += " from " + quote + entity.getName() + quote;

    return query;
  }

  public boolean isGeospatial(List<String> geospatialTypes, String type) {
    return geospatialTypes.contains(type);
  }
}
