package com.arcadeanalytics.provider.rdbms.graphprovider;

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

import com.arcadeanalytics.provider.DataSourceInfo;
import com.arcadeanalytics.provider.rdbms.dataprovider.PostgreSQLContainerHolder;
import com.arcadeanalytics.provider.rdbms.dataprovider.RDBMSGraphProvider;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;


public class PostgreSQLGraphProviderTest extends AbstractRDBMSGraphProvider {

    final PostgreSQLContainer container = PostgreSQLContainerHolder.container;

    @Test
    public void shouldFetchAllVertexes() {

        // setting the aggregationEnabled flag in the dataSource
        DataSourceInfo dataSource = new DataSourceInfo(
                1L,
                "RDBMS_POSTGRESQL",
                "testDataSource",
                "desc",
                container.getContainerIpAddress(),
                container.getFirstMappedPort(),
                container.getDatabaseName(),
                "postgres",
                "postgres",
                false,
                "{}",
                false,
                "",
                22,
                "",
                false
        );

        provider = new RDBMSGraphProvider();

        provider.provideTo(dataSource, player);
        Assert.assertEquals(44820, player.processed());

        Assert.assertEquals(44820, nodes);
        Assert.assertEquals(0, edges);

    }

    @Test
    public void shouldFetchAllVertexesExceptJoinTables() {

        // setting the aggregationEnabled flag in the dataSource
        DataSourceInfo dataSource = new DataSourceInfo(
                1L,
                "RDBMS_POSTGRESQL",
                "testDataSource",
                "desc",
                container.getContainerIpAddress(),
                container.getFirstMappedPort(),
                container.getDatabaseName(),
                "postgres",
                "postgres",
                true,
                "{}",
                false,
                "",
                22,
                "",
                false
        );

        provider = new RDBMSGraphProvider();

        provider.provideTo(dataSource, player);
        Assert.assertEquals(player.processed(), 44820);

        Assert.assertEquals(38358, nodes);
        Assert.assertEquals(6462, edges);
    }

}
