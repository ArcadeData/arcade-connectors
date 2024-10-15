package com.arcadeanalytics.provider.rdbms.persistence.util;

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

import com.arcadeanalytics.provider.DataSourceInfo;
import com.arcadeanalytics.provider.rdbms.exception.RDBMSProviderRuntimeException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to which connection with source DB is delegated.
 *
 * @author Gabriele Ponzi
 */

public class DBSourceConnection {

    private static final Logger log = LoggerFactory.getLogger(DBSourceConnection.class);

    private static Map<String, String> type2template = new HashMap<String, String>() {
        {
            put("RDBMS_HSQL", "jdbc:hsqldb:{server}:{database}");
            put("RDBMS_DATA_WORLD", "jdbc:data:world:sql:{server}:{database}");
            put("RDBMS_POSTGRESQL", "jdbc:postgresql://{server}:{port}/{database}");
            put("RDBMS_MYSQL", "jdbc:mysql://{server}:{port}/{database}?nullNamePatternMatchesAll=true&autoReconnect=true&useSSL=false");
            put("RDBMS_MSSQLSERVER", "jdbc:sqlserver://{server}:{port};databaseName={database}");
            put("RDBMS_ORACLE", "jdbc:oracle:thin:@{server}:{port}:{database}");
        }
    };

    /**
     * Gets connection according to all the source database info passed as parameter.
     *
     * @param datasource the ds
     * @return a connection
     */
    public static Connection getConnection(DataSourceInfo datasource) {
        String uri = createConnectionUrl(datasource);
        log.debug("getting connection for:: {} ", uri);

        Properties props = new Properties();
        props.setProperty("user", datasource.getUsername());
        props.setProperty("password", datasource.getPassword());

        try {
            Map<String, String> connectionAdditionalProperties = new ObjectMapper()
                    .readValue(Optional.ofNullable(datasource.getConnectionProperties())
                            .orElse("{}"), HashMap.class);

            if (connectionAdditionalProperties.size() > 0) {
                for (String connectionProp : connectionAdditionalProperties.keySet()) {
                    props.setProperty(connectionProp, connectionAdditionalProperties.get(connectionProp));
                }
            }

            Connection connection = DriverManager.getConnection(uri, props);
            return connection;
        } catch (Exception e) {
            throw new RDBMSProviderRuntimeException(e);
        }
    }

    public static String createConnectionUrl(DataSourceInfo datasource) {
        return type2template
                .get(datasource.getType())
                .replace("{server}", datasource.getServer())
                .replace("{port}", String.valueOf(datasource.getPort()))
                .replace("{database}", datasource.getDatabase());
    }
}
