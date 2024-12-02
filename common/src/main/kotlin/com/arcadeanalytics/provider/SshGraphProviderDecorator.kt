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

import com.arcadeanalytics.data.SpritePlayer
import org.slf4j.LoggerFactory

class SshGraphProviderDecorator(
    private val provider: DataSourceGraphProvider,
) : SshTunnelTemplate(),
    DataSourceGraphProvider {
    private val log = LoggerFactory.getLogger(SshGraphProviderDecorator::class.java)

    override fun provideTo(
        dataSource: DataSourceInfo,
        player: SpritePlayer,
    ) {
        val (session, wrapper) = buildTunnel(dataSource)

        provider.provideTo(wrapper, player)

        session.disconnect()
    }
}
