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
package com.arcadeanalytics.provider.orient3

import com.arcadeanalytics.provider.DataSourceInfo
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import org.testcontainers.containers.OrientDBContainer

class OrientDB3ConnectionTest {

    private val container = OrientDBContainer(ORIENTDB_DOCKER_IMAGE)
        .withServerPassword(ORIENTDB_ROOT_PASSWORD)
        .apply {
            start()
        }

    private val provider: OrientDB3DataSourceGraphDataProvider

    private var dataSource: DataSourceInfo

    init {

        dataSource = DataSourceInfo(
            id = 1L,
            type = "ORIENTDB",
            name = "testDataSource",
            server = container.containerIpAddress,
            port = container.firstMappedPort,
            username = "admin",
            password = "admin",
            database = OrientDB3DataSourceGraphDataProviderIntTest::class.java.simpleName,
        )

        val dbUrl = createTestDatabase(container.serverUrl, dataSource.database)

        createPersonSchema(dbUrl, dataSource)

        provider = OrientDB3DataSourceGraphDataProvider()
    }

    @Test
    internal fun shouldTestConnection() {
        Assertions.assertThat(provider.testConnection(dataSource)).isTrue()

        container.stop()

        Assertions.assertThatExceptionOfType(RuntimeException::class.java)
            .isThrownBy { provider.testConnection(dataSource) }
    }
}
