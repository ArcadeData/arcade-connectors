package com.arcadeanalytics.provider.rdbms.persistence.util;

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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Encapsulates the query results with the correspondent statement and connection as the super class Query Result.
 * This class is used to collect all the info about the query performed to count the navigable connections of each record.
 *
 * @author Gabriele Ponzi
*/

public class RelationshipQueryResult extends QueryResult {

    private String relationshipName;    // usually corresponds to the name of the correspondent edge class

    public RelationshipQueryResult(Connection connection, Statement statement, ResultSet result, String originalQuery, String relationshipName) {
        super(connection, statement, result, originalQuery);
        this.relationshipName = relationshipName;
    }

    public String getRelationshipName() {
        return this.relationshipName;
    }


}
