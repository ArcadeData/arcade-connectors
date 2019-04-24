/*-
 * #%L
 * Arcade Connectors
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
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import kotlin.streams.asSequence

class DataSourceMetadataProviderFactory(pluginPath: String = "./plugins") {

    private val log = LoggerFactory.getLogger(DataSourceMetadataProviderFactory::class.java)

    private val dataProviders: MutableMap<String, DataSourceMetadataProvider> = mutableMapOf()

    init {
        val walk = Files.walk(Paths.get(pluginPath))

        val clazz = DataSourceMetadataProvider::class.java
        walk.asSequence()
                .filter { path -> Files.isRegularFile(path) }
                .map { path ->
                    log.info("scanning:: ${path.toUri().toURL()}")

                    val urlClassLoader = URLClassLoader.newInstance(arrayOf(path.toUri().toURL()), javaClass.classLoader)

                    ServiceLoader.load(clazz, urlClassLoader)

                }.forEach { loader ->

                    dataProviders.putAll(loader
                            .map { it.supportedDataSourceTypes().associate { k -> k to it } }
                            .flatMap { it.entries }
                            .map { it.key to it.value }
                            .onEach { log.info("registering {} ", it.first) }
                            .toMap()
                    )

                }
    }


    fun create(dataSourceInfo: DataSourceInfo): DataSourceMetadataProvider {

        if (!dataProviders.containsKey(dataSourceInfo.type)) throw  RuntimeException("data source type not supported " + dataSourceInfo.type)

        if (dataSourceInfo.remote) return SshTunnelMetadataProviderDecorator(this)

        return dataProviders.get(dataSourceInfo.type)!!
    }

    fun provides(): Set<String> = dataProviders.keys

}
