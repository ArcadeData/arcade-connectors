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

import com.arcadeanalytics.data.SpritePlayer
import org.slf4j.LoggerFactory

class SshTunnelDataSourceGraphProviderDecorator(private val factory: DataSourceGraphProviderFactory) : SshTunnelTemplate(), DataSourceGraphProvider {


    override fun provideTo(dataSource: DataSourceInfo, processor: SpritePlayer) {

        val localPort = findFreePort()

        log.info("local port for tunnel is {}", localPort)
        val session = buildTunnel(dataSource, localPort)

        log.info("ssh session connected:: {}", session.isConnected)
        val wrapper = createLocalhostDataSource(dataSource, localPort)

        val provider = factory.create(wrapper)
        provider.provideTo(wrapper, processor)

        log.info("ssh disconnecting:: {}", session.isConnected)
        session.disconnect()


    }

    companion object {

        private val log = LoggerFactory.getLogger(SshTunnelDataSourceGraphProviderDecorator::class.java)
    }

}
