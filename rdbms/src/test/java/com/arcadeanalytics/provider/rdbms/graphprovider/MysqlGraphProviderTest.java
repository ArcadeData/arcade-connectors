package com.arcadeanalytics.provider.rdbms.graphprovider;

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

import static org.assertj.core.api.Assertions.assertThat;

import com.arcadeanalytics.provider.DataSourceInfo;
import com.arcadeanalytics.provider.rdbms.dataprovider.PostgreSQLDataProviderTest;
import com.arcadeanalytics.provider.rdbms.dataprovider.RDBMSGraphProvider;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

public class MysqlGraphProviderTest extends AbstractRDBMSGraphProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLDataProviderTest.class);
    private static final String driver = "com.mysql.cj.jdbc.Driver";
    private static final String username = "test";
    private static final String password = "test";
    public static MySQLContainer container = new MySQLContainer(DockerImageName.parse("arcadeanalytics/mysql-sakila").asCompatibleSubstituteFor("mysql"))
        .withUsername(username)
        .withPassword(password)
        .withDatabaseName("sakila");

    @BeforeAll
    public static void beforeClass() throws Exception {
        container.start();
        container.withDatabaseName("sakila");
    }

    @Test
    public void shouldFetchAllVertexes() {
        // setting the aggregationEnabled flag in the dataSource

        DataSourceInfo dataSource = new DataSourceInfo(
            1L,
            "RDBMS_MYSQL",
            "testDataSource",
            "desc",
            container.getContainerIpAddress(),
            container.getFirstMappedPort(),
            container.getDatabaseName(),
            username,
            password,
            false,
            "{}",
            false,
            false,
            "",
            22,
            "",
            false
        );
        provider = new RDBMSGraphProvider();

        provider.provideTo(dataSource, player);

        assertThat(player.processed()).isEqualTo(47273);

        Assert.assertEquals(47273, nodes);
        Assert.assertEquals(0, edges);
    }

    @Test
    public void shouldFetchAllVertexesExceptJoinTables() {
        // setting the aggregationEnabled flag in the dataSource
        DataSourceInfo dataSource = new DataSourceInfo(
            1L,
            "RDBMS_MYSQL",
            "testDataSource",
            "desc",
            container.getContainerIpAddress(),
            container.getFirstMappedPort(),
            container.getDatabaseName(),
            username,
            password,
            true,
            "{}",
            false,
            false,
            "",
            22,
            "",
            false
        );

        provider = new RDBMSGraphProvider();

        provider.provideTo(dataSource, player);

        assertThat(player.processed()).isEqualTo(47273);

        Assert.assertEquals(40811, nodes);
        Assert.assertEquals(6462, edges);
    }
}
