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

internal class DataSourceTableDataProviderFactoryTest {

    companion object {
        @JvmStatic
        fun types2implementation() = listOf(
            Arguments.of("ORIENTDB3", "OrientDB3DataSourceTableDataProvider"),
            Arguments.of("RDBMS_POSTGRESQL", "RDBMSTableDataProvider"),
            Arguments.of("RDBMS_MYSQL", "RDBMSTableDataProvider"),
            Arguments.of("RDBMS_MSSQLSERVER", "RDBMSTableDataProvider"),
            Arguments.of("RDBMS_HSQL", "RDBMSTableDataProvider"),
            Arguments.of("RDBMS_ORACLE", "RDBMSTableDataProvider"),
            Arguments.of("RDBMS_DATA_WORLD", "RDBMSTableDataProvider"),
        )
    }

    private lateinit var factory: DataSourceProviderFactory<DataSourceTableDataProvider>

    @BeforeEach
    internal fun setUp() {
        factory = DataSourceProviderFactory(DataSourceTableDataProvider::class.java)
    }

    @Test
    internal fun `should provides all apis`() {
        assertThat(factory.provides())
            .hasSize(7)
            .contains(
                "RDBMS_POSTGRESQL",
                "RDBMS_MYSQL",
                "RDBMS_MSSQLSERVER",
                "RDBMS_HSQL",
                "RDBMS_ORACLE",
                "RDBMS_DATA_WORLD",
                "ORIENTDB3",
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
