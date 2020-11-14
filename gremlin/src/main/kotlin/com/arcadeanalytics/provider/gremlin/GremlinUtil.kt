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
package com.arcadeanalytics.provider.gremlin

import com.arcadeanalytics.provider.DataSourceInfo
import org.apache.tinkerpop.gremlin.driver.Cluster
import org.apache.tinkerpop.gremlin.driver.MessageSerializer
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV1d0
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0
import org.apache.tinkerpop.gremlin.orientdb.io.OrientIoRegistry
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry

fun getCluster(dataSource: DataSourceInfo): Cluster =
    Cluster.build(dataSource.server)
        .port(dataSource.port)
        .serializer(createSerializer(dataSource))
        .enableSsl(dataSource.enableSsl)
        .sslSkipCertValidation(dataSource.skipCertValidation)
        .maxContentLength(65536 * 4)
        .credentials(dataSource.username, dataSource.password)
        .maxWaitForConnection(20000)
        .create()

fun splitMultilabel(label: String): String =
    label.split("::")
        .joinToString("','", "'", "'")

fun createSerializer(dataSource: DataSourceInfo): MessageSerializer {
    return when (dataSource.type) {
        "GREMLIN_ORIENTDB" ->
            GryoMessageSerializerV3d0(
                GryoMapper.build()
                    .addRegistry(OrientIoRegistry.getInstance())
            )
        "GREMLIN_NEPTUNE" -> GryoMessageSerializerV3d0()
        "GREMLIN_JANUSGRAPH" ->
            GryoMessageSerializerV3d0(
                GryoMapper.build()
                    .addRegistry(JanusGraphIoRegistry.getInstance())
            )
        "GREMLIN_COSMOSDB" -> {
            val serializer = GraphSONMessageSerializerV1d0()
            serializer.configure(mapOf("serializeResultToString" to true), null)
            serializer
        }
        else -> throw RuntimeException("requested type not implemented yet:: ${dataSource.type}")
    }
}
