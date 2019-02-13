package com.arcadeanalytics.provider.orient2

import com.arcadeanalytics.provider.DataSourceInfo
import com.arcadeanalytics.provider.orientdb.OrientDBDataSourceGraphDataProviderIntTest
import com.arcadeanalytics.provider.orientdb.OrientDBTestHelper
import com.arcadeanalytics.test.KGenericContainer
import org.testcontainers.containers.wait.strategy.Wait

object OrientDBContainer {

    val container: KGenericContainer = KGenericContainer(OrientDBTestHelper.ORIENTDB_DOCKER_IMAGE)
            .apply {
                withExposedPorts(2424)
                withEnv("ORIENTDB_ROOT_PASSWORD", OrientDBTestHelper.ORIENTDB_ROOT_PASSWORD)
                waitingFor(Wait.forListeningPort())
                start()

            }


    val dataSource: DataSourceInfo

    var dbUrl: String

    init {
        dataSource = DataSourceInfo(id = 1L,
                type = "ORIENTDB",
                name = "testDataSource",
                server = container.containerIpAddress,
                port = container.firstMappedPort,
                username = "admin",
                password = "admin",
                database = OrientDBDataSourceGraphDataProviderIntTest::class.java.simpleName
        )


        val serverUrl = OrientDBTestHelper.getServerUrl(container)

        dbUrl = OrientDBTestHelper.createTestDataabse(serverUrl, dataSource.database)

        OrientDBTestHelper.createPersonSchema(dbUrl)

    }


}