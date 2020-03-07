package com.arcadeanalytics.provider.rdbms.persistence.util;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Encapsulates the query results with the correspondent statement and connection.
 *
 * @author Gabriele Ponzi
 */

public class QueryResult {

    private final Logger log = LoggerFactory.getLogger(QueryResult.class);
    private final Connection dbConnection;
    private final Statement statement;
    private final ResultSet result;
    private final String originalQuery;

    public QueryResult(Connection connection, Statement statement, ResultSet result, String originalQuery) {
        this.dbConnection = connection;
        this.statement = statement;
        this.result = result;
        this.originalQuery = originalQuery;
    }

    public Connection getDbConnection() {
        return this.dbConnection;
    }

    public Statement getStatement() {
        return this.statement;
    }

    public ResultSet getResult() {
        return result;
    }

    public String getOriginalQuery() {
        return this.originalQuery;
    }


    public void close() {

        try {
            statement.close();
            result.close();
        } catch (SQLException e) {
            log.error("", e);
        }
    }

    public boolean isConnectionClosed() {
        try {
            return dbConnection.isClosed();
        } catch (Exception e) {
            log.error("", e);
        }
        return false;
    }

    public boolean isStatementClosed() {
        try {
            return statement.isClosed();
        } catch (Exception e) {
            log.error("", e);
        }
        return false;
    }

    public boolean isResultSetClosed() {
        try {
            return result.isClosed();
        } catch (Exception e) {
            log.error("", e);
        }
        return false;
    }

    public boolean isAllClosed() {
        if (isConnectionClosed() && isStatementClosed() && isResultSetClosed())
            return true;
        else
            return false;
    }

}
