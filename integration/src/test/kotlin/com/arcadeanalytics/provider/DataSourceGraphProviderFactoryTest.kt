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
package com.arcadeanalytics.provider

import com.arcadeanalytics.provider.gremlin.GremlinGraphProvider
import com.arcadeanalytics.provider.gremlin.cosmosdb.CosmosDBGremlinGraphProvider
import com.arcadeanalytics.provider.neo4j3.Neo4jGraphProvider
import com.arcadeanalytics.provider.orient2.OrientDBDataSourceGraphProvider
import com.arcadeanalytics.provider.rdbms.dataprovider.RDBMSGraphProvider
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
                Arguments.of("ORIENTDB", OrientDBDataSourceGraphProvider::class.java),
                Arguments.of("GREMLIN_ORIENTDB", GremlinGraphProvider::class.java),
                Arguments.of("GREMLIN_NEPTUNE", GremlinGraphProvider::class.java),
                Arguments.of("GREMLIN_JANUSGRAPH", GremlinGraphProvider::class.java),
                Arguments.of("GREMLIN_COSMOSDB", CosmosDBGremlinGraphProvider::class.java),
                Arguments.of("NEO4J", Neo4jGraphProvider::class.java),
                Arguments.of("NEO4J_MEMGRAPH", Neo4jGraphProvider::class.java),
                Arguments.of("RDBMS_MYSQL", RDBMSGraphProvider::class.java),
                Arguments.of("RDBMS_MSSQLSERVER", RDBMSGraphProvider::class.java),
                Arguments.of("RDBMS_ORACLE", RDBMSGraphProvider::class.java),
                Arguments.of("RDBMS_HSQL", RDBMSGraphProvider::class.java),
                Arguments.of("RDBMS_DATA_WORLD", RDBMSGraphProvider::class.java)

        )

    }

    private lateinit var factory: DataSourceGraphProviderFactory

    @BeforeEach
    internal fun setUp() {
        factory = DataSourceGraphProviderFactory()

    }

    @Test
    internal fun `should provides all apis`() {
        assertThat(factory.provides())
                .hasSize(13)
                .contains("ORIENTDB",
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
                        "RDBMS_DATA_WORLD"
                )

    }

    @ParameterizedTest
    @MethodSource("types2implementation")
    internal fun `should create right data provider for given data source type`(
            type: String,
            impl: Class<DataSourceGraphDataProvider>) {

        val dataSource = DataSourceInfo(id = 1L,
                type = type,
                name = "testDataSource",
                server = "1.2.3.4",
                port = 1234,
                username = "admin",
                password = "admin",
                database = "testDb")

        Assertions.assertThat(factory.create(dataSource)).isNotNull
                .isInstanceOf(impl)
    }
}
