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

import com.arcadeanalytics.provider.rdbms.model.dbschema.Entity;
import java.util.List;

/**
 * Query Builder for MySQL DBMS. It extends the CommonQueryBuilder class and overrides only the
 * needed methods.
 *
 * @author Gabriele Ponzi
 */
public class MysqlQueryBuilder extends CommonQueryBuilder {

  public MysqlQueryBuilder() {
    this.quote = "`";
  }

  /**
   * MySQL does not allow full outer join, so this query is expressed as UNION of LEFT and RIGHT
   * JOIN.
   *
   * @param mappedEntities the mapped entities
   * @param columns colums
   * @return query produced
   */
  @Override
  public String getRecordsFromMultipleEntities(List<Entity> mappedEntities, String[][] columns) {
    String query;

    Entity first = mappedEntities.get(0);
    if (first.getSchemaName() != null)
      query =
          "select * from "
              + first.getSchemaName()
              + "."
              + this.quote
              + first.getName()
              + this.quote
              + " as t0\n";
    else query = "select * from " + this.quote + first.getName() + this.quote + " as t0\n";

    for (int i = 1; i < mappedEntities.size(); i++) {
      Entity currentEntity = mappedEntities.get(i);
      query +=
          " left join "
              + currentEntity.getSchemaName()
              + "."
              + this.quote
              + currentEntity.getName()
              + this.quote
              + " as t"
              + i;
      query +=
          " on t"
              + (i - 1)
              + "."
              + this.quote
              + columns[i - 1][0]
              + this.quote
              + " = t"
              + i
              + "."
              + this.quote
              + columns[i][0]
              + this.quote;

      for (int k = 1; k < columns[i].length; k++) {
        query +=
            " and t"
                + (i - 1)
                + "."
                + this.quote
                + columns[i - 1][k]
                + this.quote
                + " = t"
                + i
                + "."
                + this.quote
                + columns[i][k]
                + this.quote;
      }

      query += "\n";
    }

    query += "UNION\n";

    if (first.getSchemaName() != null)
      query +=
          "select * from "
              + first.getSchemaName()
              + "."
              + this.quote
              + first.getName()
              + this.quote
              + " as t0\n";
    else query += "select * from " + this.quote + first.getName() + this.quote + " as t0\n";

    for (int i = 1; i < mappedEntities.size(); i++) {
      Entity currentEntity = mappedEntities.get(i);
      query +=
          " right join "
              + currentEntity.getSchemaName()
              + "."
              + this.quote
              + currentEntity.getName()
              + this.quote
              + " as t"
              + i;
      query +=
          " on t"
              + (i - 1)
              + "."
              + this.quote
              + columns[i - 1][0]
              + this.quote
              + " = t"
              + i
              + "."
              + this.quote
              + columns[i][0]
              + this.quote;

      for (int k = 1; k < columns[i].length; k++) {
        query +=
            " and t"
                + (i - 1)
                + "."
                + this.quote
                + columns[i - 1][k]
                + this.quote
                + " = t"
                + i
                + "."
                + this.quote
                + columns[i][k]
                + this.quote;
      }

      query += "\n";
    }

    return query;
  }
}
