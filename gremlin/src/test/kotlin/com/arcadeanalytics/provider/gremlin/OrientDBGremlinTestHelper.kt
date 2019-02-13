package com.arcadeanalytics.provider.gremlin

import com.arcadeanalytics.provider.DataSourceInfo
import com.arcadeanalytics.test.KGenericContainer
import org.testcontainers.containers.wait.strategy.Wait

object OrientDBGremlinContainer {

    val container: KGenericContainer = KGenericContainer("arcade/orientdb:3.0.13-tp3")
            .apply {

                withExposedPorts(8182, 2424)
                withEnv("ORIENTDB_ROOT_PASSWORD", "arcade")
                waitingFor(Wait.defaultWaitStrategy())
                start()
            }

    val dataSource: DataSourceInfo

    init {
        dataSource = DataSourceInfo(id = 1L,
                type = "GREMLIN_ORIENTDB",
                name = "testDataSource",
                server = container.containerIpAddress,
                port = container.firstMappedPort,
                username = "root",
                password = "arcade",
                database = "demodb"
        )

    }
}