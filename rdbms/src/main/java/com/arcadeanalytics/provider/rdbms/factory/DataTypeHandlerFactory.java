package com.arcadeanalytics.provider.rdbms.factory;

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

import com.arcadeanalytics.provider.rdbms.persistence.handler.DBMSDataTypeHandler;
import com.arcadeanalytics.provider.rdbms.persistence.handler.HSQLDBDataTypeHandler;
import com.arcadeanalytics.provider.rdbms.persistence.handler.MySQLDataTypeHandler;
import com.arcadeanalytics.provider.rdbms.persistence.handler.OracleDataTypeHandler;
import com.arcadeanalytics.provider.rdbms.persistence.handler.PostgreSQLDataTypeHandler;
import com.arcadeanalytics.provider.rdbms.persistence.handler.SQLServerDataTypeHandler;

/**
 * Factory used to instantiate a specific DataTypeHandler according to the driver of the
 * DBMS from which the import is performed.
 *
 * @author Gabriele Ponzi
 */

public class DataTypeHandlerFactory {

  public DBMSDataTypeHandler buildDataTypeHandler(String type) {
    switch (type) {
      case "RDBMS_ORACLE":
        return new OracleDataTypeHandler();
      case "RDBMS_MSQSLSERVER":
        return new SQLServerDataTypeHandler();
      case "RDBMS_MYSQL":
        return new MySQLDataTypeHandler();
      case "RDBMS_POSTGRESQL":
        return new PostgreSQLDataTypeHandler();
      case "RDBMS_HSQL":
        return new HSQLDBDataTypeHandler();
      default:
        return new DBMSDataTypeHandler();
    }
  }
}
