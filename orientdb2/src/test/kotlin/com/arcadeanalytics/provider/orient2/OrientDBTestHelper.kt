package com.arcadeanalytics.provider.orientdb

import com.orientechnologies.orient.client.remote.OServerAdmin
import com.orientechnologies.orient.core.command.script.OCommandScript
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import org.testcontainers.containers.GenericContainer
import java.io.IOException

/**
 * Helper class for tests on OrientDB
 */
object OrientDBTestHelper {

    val ORIENTDB_DOCKER_IMAGE = "orientdb:2.2.36"
    val ORIENTDB_TP3_DOCKER_IMAGE = "arcade/orientdb:3.0.13-tp3"
    val ORIENTDB_ROOT_PASSWORD = "arcade"

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
    fun createPersonSchema(dbUrl: String) {
        ODatabaseDocumentTx(dbUrl)
                .open<ODatabaseDocumentTx>("admin", "admin")
                .use {
                    it.command(OCommandScript("sql", """

                    CREATE CLASS Person EXTENDS V;

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
                    )).execute<Any>()
                }

    }

    /**
     * Creates the "test" database on the given server
     *
     * @param serverUrl
     * @return
     */
    fun createTestDataabse(serverUrl: String, dbname: String): String {

        try {
            OServerAdmin(serverUrl).apply {
                connect("root", ORIENTDB_ROOT_PASSWORD)
                createDatabase(dbname, "graph", "plocal")
                close()
            }
            return "$serverUrl/$dbname"
        } catch (e: IOException) {
            throw RuntimeException("unable to create database on " + serverUrl, e)
        }


    }
}

