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
package com.arcadeanalytics.provider.orient2

import com.arcadeanalytics.provider.DataSourceInfo
import com.arcadeanalytics.provider.orientdb.OrientDBDataSourceGraphDataProviderIntTest
import com.arcadeanalytics.test.KGenericContainer
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import org.testcontainers.containers.wait.strategy.Wait

class OrientDBConnectionTest {

    private val container: KGenericContainer = KGenericContainer(ORIENTDB_DOCKER_IMAGE)
        .apply {
            withExposedPorts(2424)
            withEnv("ORIENTDB_ROOT_PASSWORD", ORIENTDB_ROOT_PASSWORD)
            waitingFor(Wait.forListeningPort())
            start()
        }

    private val provider: OrientDBDataSourceGraphDataProvider

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
            database = OrientDBDataSourceGraphDataProviderIntTest::class.java.simpleName
        )

        val serverUrl = getServerUrl(container)

        val dbUrl = createTestDatabase(serverUrl, OrientDBDataSourceGraphDataProviderIntTest::class.java.simpleName)

        createPersonSchema(dbUrl)

        provider = OrientDBDataSourceGraphDataProvider()
    }

    @Test
    internal fun shouldTestConnection() {
        Assertions.assertThat(provider.testConnection(dataSource)).isTrue()

        container.stop()

        Assertions.assertThatExceptionOfType(RuntimeException::class.java)
            .isThrownBy { provider.testConnection(dataSource) }
    }
}
