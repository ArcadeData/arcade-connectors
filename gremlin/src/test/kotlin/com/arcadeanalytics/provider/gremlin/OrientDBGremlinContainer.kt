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
package com.arcadeanalytics.provider.gremlin

import com.arcadeanalytics.provider.DataSourceInfo
import com.arcadeanalytics.test.KGenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

object OrientDBGremlinContainer {
    private val container: KGenericContainer =
        KGenericContainer(DockerImageName.parse("arcadeanalytics/orientdb3"))
            .apply {

                withExposedPorts(8182, 2424)
                withEnv("ORIENTDB_ROOT_PASSWORD", "arcade")
                waitingFor(Wait.defaultWaitStrategy())
                start()
            }

    val dataSource: DataSourceInfo

    init {
        dataSource =
            DataSourceInfo(
                id = 1L,
                type = "GREMLIN_ORIENTDB",
                name = "testDataSource",
                server = container.containerIpAddress,
                port = container.firstMappedPort,
                username = "root",
                password = "arcade",
                database = "demodb",
            )
    }
}
