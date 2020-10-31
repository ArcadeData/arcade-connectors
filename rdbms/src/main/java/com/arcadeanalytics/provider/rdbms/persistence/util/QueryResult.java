package com.arcadeanalytics.provider.rdbms.persistence.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    if (isConnectionClosed() && isStatementClosed() && isResultSetClosed()) return true; else return false;
  }
}
