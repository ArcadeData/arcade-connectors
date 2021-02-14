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

package com.arcadeanalytics.provider.orient3

import com.arcadeanalytics.provider.DataSourceInfo
import com.orientechnologies.orient.core.db.ODatabaseType
import com.orientechnologies.orient.core.db.OrientDB
import com.orientechnologies.orient.core.db.OrientDBConfig
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.OrientDBContainer
import org.testcontainers.utility.DockerImageName
import java.io.IOException

val ORIENTDB_DOCKER_IMAGE = DockerImageName.parse("arcadeanalytics/orientdb3:latest")
    .asCompatibleSubstituteFor("orientdb")

const val ORIENTDB_ROOT_PASSWORD = "arcade"

object OrientDB3Container {

    private val container = OrientDBContainer(ORIENTDB_DOCKER_IMAGE)
        .withServerPassword(ORIENTDB_ROOT_PASSWORD)
        .apply {
            start()
        }

    val dataSource: DataSourceInfo = DataSourceInfo(
        id = 1L,
        type = "ORIENTDB",
        name = "testDataSource",
        server = container.containerIpAddress,
        port = container.firstMappedPort,
        username = "admin",
        password = "admin",
        database = "testDatabase"
    )

    val dbUrl: String = createTestDatabase(container.serverUrl, dataSource.database)

    init {

        createPersonSchema(dbUrl, dataSource)
    }

    fun getContainer(): OrientDBContainer {
        return container
    }
}

/**
 * Given an OrientDB container instance, returns the remote url
 *
 * @param container
 * @return
 */
fun getServerUrl(container: GenericContainer<*>): String {

    return "remote:${container.getContainerIpAddress()}:${container.getMappedPort(2424)}"
}

/**
 * Given an OrientDB's database url, creates the Person schema and fills it with samples data
 *
 * @param dbUrl
 */
fun createPersonSchema(dbUrl: String, dataSource: DataSourceInfo) {

    val command: String =
        """

                    CREATE CLASS Person EXTENDS V;

                    CREATE PROPERTY Person.name STRING;
                    CREATE PROPERTY Person.age INTEGER;
                    CREATE INDEX Person.name ON Person(name) UNIQUE;

                    CREATE CLASS FriendOf EXTENDS E;
                    CREATE PROPERTY FriendOf.kind STRING;

                    CREATE CLASS HaterOf EXTENDS E;
                    CREATE PROPERTY HaterOf.kind STRING;

                    INSERT INTO Person SET name='rob', age='45';
                    INSERT INTO Person SET name='frank', age='45';
                    INSERT INTO Person SET name='john', age='35';
                    INSERT INTO Person SET name='jane', age='34';

                    CREATE EDGE FriendOf FROM (SELECT FROM Person WHERE name = 'rob') TO (SELECT FROM Person WHERE name = 'frank') set kind='fraternal';
                    CREATE EDGE FriendOf FROM (SELECT FROM Person WHERE name = 'john') TO (SELECT FROM Person WHERE name = 'jane') set kind='fraternal';
                    CREATE EDGE HaterOf FROM (SELECT FROM Person WHERE name = 'jane') TO (SELECT FROM Person WHERE name = 'rob') set kind='killer';
                    CREATE EDGE HaterOf FROM (SELECT FROM Person WHERE name = 'frank') TO (SELECT FROM Person WHERE name = 'john') set kind='killer';
        """.trimIndent()

    val orientDB = OrientDB(dbUrl, OrientDBConfig.defaultConfig())

    orientDB.open(dataSource.database, dataSource.username, dataSource.password)
        .use {
            it.execute("sql", command)
        }
}

/**
 * Creates the "test" database on the given server
 *
 * @param serverUrl
 * @return
 */
fun createTestDatabase(serverUrl: String, dbname: String): String {

    try {

        val orientDB = OrientDB(serverUrl, "root", ORIENTDB_ROOT_PASSWORD, OrientDBConfig.defaultConfig())
        orientDB.create(dbname, ODatabaseType.PLOCAL)
        orientDB.close()

        return "$serverUrl/$dbname"
    } catch (e: IOException) {
        throw RuntimeException("unable to create database on " + serverUrl, e)
    }
}
