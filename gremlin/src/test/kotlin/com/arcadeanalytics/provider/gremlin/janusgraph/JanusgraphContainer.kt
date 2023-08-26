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
package com.arcadeanalytics.provider.gremlin.janusgraph

import com.arcadeanalytics.provider.DataSourceInfo
import com.arcadeanalytics.provider.gremlin.getCluster
import com.arcadeanalytics.test.KGenericContainer
import org.apache.tinkerpop.gremlin.driver.Client
import org.janusgraph.core.JanusGraph
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

object JanusgraphContainer {

    private val container: KGenericContainer =
        KGenericContainer(DockerImageName.parse("janusgraph/janusgraph:${JanusGraph.version()}"))
            .apply {
                withExposedPorts(8182)
                waitingFor(Wait.defaultWaitStrategy())
                start()
            }

    val dataSource: DataSourceInfo = DataSourceInfo(
        id = 1L,
        type = "GREMLIN_JANUSGRAPH",
        name = "testDataSource",
        server = container.containerIpAddress,
        port = container.firstMappedPort,
        username = "",
        password = "",
        database = "",
    )

    init {

        val cluster = getCluster(dataSource)

        var client = cluster.connect<Client>().init()

        client.submit("graph.io(graphml()).readGraph('data/grateful-dead.xml')")

        client.close()
    }
}
