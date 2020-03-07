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


package com.arcadeanalytics.provider.neo4j3

import com.arcadeanalytics.provider.DataSourceInfo
import org.apache.commons.lang3.StringUtils
import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.Config
import org.neo4j.driver.v1.Driver
import org.neo4j.driver.v1.GraphDatabase
import java.util.concurrent.TimeUnit


private const val connectionTemplate = "bolt://{server}:{port}"

fun toArcadeId(dataSource: DataSourceInfo, type: Neo4jType, neo4jId: Long): String {
    return dataSource.id.toString() + type.suffix() + neo4jId
}


fun toNeo4jId(dataSource: DataSourceInfo, arcadeId: String): String {

    val cleaned = StringUtils.removeStart(arcadeId, dataSource.id.toString() + Neo4jType.NODE.suffix())

    return StringUtils.removeStart(cleaned, dataSource.id.toString() + Neo4jType.EDGE.suffix())
}

fun createConnectionUrl(datasource: DataSourceInfo): String {
    return connectionTemplate.replace("{server}", datasource.server)
            .replace("{port}", datasource.port.toString())
}

fun getDriver(datasource: DataSourceInfo): Driver {

    val connectionUrl = createConnectionUrl(datasource)

    val config = Config.build()
            .withConnectionTimeout(30, TimeUnit.SECONDS)
            .withMaxConnectionPoolSize(1)
            .toConfig()


    return GraphDatabase.driver(connectionUrl,
            AuthTokens.basic(datasource.username, datasource.password), config)

}


enum class Neo4jType {
    NODE {
        override fun suffix(): String {
            return "_n_"
        }
    },
    EDGE {
        override fun suffix(): String {
            return "_e_"
        }
    };


    abstract fun suffix(): String

}

enum class Neo4jDialect {
    NEO4J, NEO4J_MEMGRAPH
}

