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
package com.arcadeanalytics.provider

import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource

internal class DataSourceGraphProviderFactoryTest {

    companion object {
        @JvmStatic
        fun types2implementation() = listOf(
            Arguments.of("ORIENTDB", "OrientDBDataSourceGraphProvider"),
            Arguments.of("ORIENTDB3", "OrientDB3DataSourceGraphProvider"),
            Arguments.of("GREMLIN_ORIENTDB", "GremlinGraphProvider"),
            Arguments.of("GREMLIN_NEPTUNE", "GremlinGraphProvider"),
            Arguments.of("GREMLIN_JANUSGRAPH", "GremlinGraphProvider"),
            Arguments.of("GREMLIN_COSMOSDB", "CosmosDBGremlinGraphProvider"),
            Arguments.of("NEO4J", "Neo4jGraphProvider"),
            Arguments.of("NEO4J_MEMGRAPH", "Neo4jGraphProvider"),
            Arguments.of("RDBMS_MYSQL", "RDBMSGraphProvider"),
            Arguments.of("RDBMS_MSSQLSERVER", "RDBMSGraphProvider"),
            Arguments.of("RDBMS_ORACLE", "RDBMSGraphProvider"),
            Arguments.of("RDBMS_HSQL", "RDBMSGraphProvider"),
            Arguments.of("RDBMS_DATA_WORLD", "RDBMSGraphProvider"),

        )
    }

    private lateinit var factory: DataSourceProviderFactory<DataSourceGraphProvider>

    @BeforeEach
    internal fun setUp() {
        factory = DataSourceProviderFactory(DataSourceGraphProvider::class.java)
    }

    @Test
    internal fun `should provides all apis`() {
        assertThat(factory.provides())
            .hasSize(14)
            .contains(
                "ORIENTDB",
                "ORIENTDB3",
                "NEO4J",
                "NEO4J_MEMGRAPH",
                "GREMLIN_ORIENTDB",
                "GREMLIN_NEPTUNE",
                "GREMLIN_COSMOSDB",
                "GREMLIN_JANUSGRAPH",
                "RDBMS_POSTGRESQL",
                "RDBMS_MYSQL",
                "RDBMS_MSSQLSERVER",
                "RDBMS_HSQL",
                "RDBMS_ORACLE",
                "RDBMS_DATA_WORLD",
            )
    }

    @ParameterizedTest
    @MethodSource("types2implementation")
    internal fun `should create right data provider for given data source type`(
        type: String,
        impl: String,
    ) {
        val dataSource = DataSourceInfo(
            id = 1L,
            type = type,
            name = "testDataSource",
            server = "1.2.3.4",
            port = 1234,
            username = "admin",
            password = "admin",
            database = "testDb",
        )

        val provider = factory.create(dataSource)
        Assertions.assertThat(provider).isNotNull

        Assertions.assertThat(provider::class.java.simpleName).isEqualTo(impl)
    }
}
