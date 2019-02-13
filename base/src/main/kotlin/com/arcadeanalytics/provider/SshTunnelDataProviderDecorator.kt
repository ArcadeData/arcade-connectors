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

import org.slf4j.LoggerFactory

class SshTunnelDataProviderDecorator(private val factory: DataSourceGraphDataProviderFactory) : SshTunnelTemplate(), DataSourceGraphDataProvider {

    private val log = LoggerFactory.getLogger(SshTunnelDataProviderDecorator::class.java)


    override fun testConnection(dataSource: DataSourceInfo): Boolean {
        val localPort = findFreePort()

        val session = buildTunnel(dataSource, localPort)

        val wrapper = createLocalhostDataSource(dataSource, localPort)

        val provider = factory.create(wrapper)

        val testConnection = provider.testConnection(wrapper)

        session.disconnect()

        return testConnection

    }

    override fun fetchData(dataSource: DataSourceInfo, query: String, limit: Int): GraphData {

        val localPort = findFreePort()

        val session = buildTunnel(dataSource, localPort)

        val wrapper = createLocalhostDataSource(dataSource, localPort)

        val provider = factory.create(wrapper)
        val graphData = provider.fetchData(wrapper, query, limit)

        session.disconnect()

        return graphData
    }


    override fun expand(dataSource: DataSourceInfo, roots: Array<String>, direction: String, edgeLabel: String, maxTraversal: Int): GraphData {
        val localPort = findFreePort()

        val session = buildTunnel(dataSource, localPort)

        val wrapper = createLocalhostDataSource(dataSource, localPort)

        val provider = factory.create(wrapper)
        val graphData = provider.expand(wrapper, roots, direction, edgeLabel, maxTraversal)

        session.disconnect()

        return graphData
    }

    override fun load(dataSource: DataSourceInfo, ids: Array<String>): GraphData {
        val localPort = findFreePort()

        val session = buildTunnel(dataSource, localPort)
        val wrapper = createLocalhostDataSource(dataSource, localPort)

        val provider = factory.create(wrapper)
        val graphData = provider.load(wrapper, ids)


        session.disconnect()

        return graphData
    }

    override fun loadFromClass(dataSource: DataSourceInfo, className: String, limit: Int): GraphData {

        val localPort = findFreePort()

        val session = buildTunnel(dataSource, localPort)

        val wrapper = createLocalhostDataSource(dataSource, localPort)

        val provider = factory.create(wrapper)
        val graphData = provider.loadFromClass(wrapper, className, limit)

        session.disconnect()

        return graphData
    }

    override fun loadFromClass(dataSource: DataSourceInfo, className: String, propName: String, propValue: String, limit: Int): GraphData {
        val localPort = findFreePort()

        val session = buildTunnel(dataSource, localPort)

        val wrapper = createLocalhostDataSource(dataSource, localPort)

        val provider = factory.create(wrapper)
        val graphData = provider.loadFromClass(wrapper, className, propName, propValue, limit)

        session.disconnect()

        return graphData
    }

}
