package com.arcadeanalytics.provider.orientdb

import com.arcadeanalytics.provider.DataSourceInfo
import com.arcadeanalytics.test.KGenericContainer
import com.orientechnologies.orient.client.remote.OServerAdmin
import com.orientechnologies.orient.core.command.script.OCommandScript
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.io.IOException

const val ORIENTDB_DOCKER_IMAGE = "orientdb:2.2.37-spatial"

const val ORIENTDB_ROOT_PASSWORD = "arcade"

object OrientDBContainer {

    private val container: KGenericContainer = KGenericContainer(ORIENTDB_DOCKER_IMAGE)
            .apply {
                withExposedPorts(2424)
                withEnv("ORIENTDB_ROOT_PASSWORD", ORIENTDB_ROOT_PASSWORD)
                waitingFor(Wait.forListeningPort())
                start()
            }

    val dataSource: DataSourceInfo

    val dbUrl: String

    init {

        dataSource = DataSourceInfo(id = 1L,
                type = "ORIENTDB",
                name = "testDataSource",
                server = container.containerIpAddress,
                port = container.firstMappedPort,
                username = "admin",
                password = "admin",
                database = "testDatabase"
        )

        val serverUrl = getServerUrl(container)

        dbUrl = createTestDatabase(serverUrl, dataSource.database)

        createPersonSchema(dbUrl)

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
fun createPersonSchema(dbUrl: String) {
    ODatabaseDocumentTx(dbUrl)
            .open<ODatabaseDocumentTx>("admin", "admin")
            .use {
                it.command(OCommandScript("sql", """

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
                )).execute<Any>()
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
        OServerAdmin(serverUrl)
                .apply {
                    connect("root", ORIENTDB_ROOT_PASSWORD)
                    createDatabase(dbname, "graph", "plocal")
                    close()
                }
        return "$serverUrl/$dbname"
    } catch (e: IOException) {
        throw RuntimeException("unable to create database on " + serverUrl, e)
    }


}


