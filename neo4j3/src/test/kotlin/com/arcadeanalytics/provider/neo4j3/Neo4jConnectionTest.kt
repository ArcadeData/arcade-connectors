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
package com.arcadeanalytics.provider.neo4j3

import com.arcadeanalytics.provider.DataSourceInfo
import com.arcadeanalytics.provider.neo.Neo4jDataProviderIntTest
import com.arcadeanalytics.provider.neo.fillDatabase
import com.arcadeanalytics.test.KGenericContainer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

class Neo4jConnectionTest {
    private val log = LoggerFactory.getLogger(Neo4jDataProviderIntTest::class.java)

    private val container: KGenericContainer =
        KGenericContainer(DockerImageName.parse("neo4j:3.5"))
            .apply {
                withExposedPorts(7687, 7474)
                withEnv("NEO4J_AUTH", "neo4j/arcade")
                waitingFor(Wait.forListeningPort())
                start()
            }

    private val provider: Neo4jDataProvider

    private val dataSource: DataSourceInfo

    init {

        container.followOutput(Slf4jLogConsumer(log))

        dataSource =
            DataSourceInfo(
                id = 1L,
                type = "NEO4J",
                name = "testDataSource",
                server = container.containerIpAddress,
                port = container.firstMappedPort,
                username = "neo4j",
                password = "arcade",
                database = Neo4jDataProviderIntTest::class.java.simpleName,
            )
        getDriver(dataSource).use { driver ->

            driver.session().use {
                fillDatabase(it)
            }
        }

        provider = Neo4jDataProvider()
    }

    @Test
    internal fun shouldCheckConnectionStatus() {
        assertThat(provider.testConnection(dataSource)).isTrue()

        container.stop()

        assertThatExceptionOfType(RuntimeException::class.java).isThrownBy { provider.testConnection(dataSource) }
    }
}
