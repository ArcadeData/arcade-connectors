package com.arcadeanalytics.provider.rdbms.dataprovider;

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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class PGSQLSandboxTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PGSQLSandboxTest.class);


    static PostgreSQLContainer container = new PostgreSQLContainer("arcade:postgres-dvdrental")
            .withUsername("postgres")
            .withPassword("postgres");


    private String dbUrl;

    @BeforeAll
    public static void beforeClass() throws Exception {
        container.start();
        container.withDatabaseName("dvdrental");
        Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(LOGGER);
//        container.followOutput(logConsumer);
    }


    @AfterAll
    public static void afterAll() {
        container.stop();
    }

    @BeforeEach
    public void setUp() throws Exception {

        dbUrl = container.getJdbcUrl();
        System.out.println("dbUrl = " + dbUrl);
    }

    @Test
    public void tryConnection() throws SQLException {

        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "postgres");
        Connection conn = DriverManager.getConnection(dbUrl, props);

        final Statement statement = conn.createStatement();
        final ResultSet resultSet = statement.executeQuery("select * from actor limit 50");

        while (resultSet.next()) {
            System.out.println("resultSet = " + resultSet);
        }

        statement.close();
        conn.close();
    }
}
