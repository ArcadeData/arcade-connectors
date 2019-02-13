package com.arcadeanalytics.provider.gremlin.janusgraph

import com.arcadeanalytics.provider.DataSourceInfo
import com.arcadeanalytics.provider.gremlin.getCluster
import com.arcadeanalytics.test.KGenericContainer
import org.apache.tinkerpop.gremlin.driver.Client
import org.testcontainers.containers.wait.strategy.Wait

object JanusgraphContainer {

    val container: KGenericContainer = KGenericContainer("arcade/janusgraph")
            .apply {
                withExposedPorts(8182)
                waitingFor(Wait.defaultWaitStrategy())
                start()
            }


    val dataSource: DataSourceInfo


    init {


        dataSource = DataSourceInfo(id = 1L,
                type = "GREMLIN_JANUSGRAPH",
                name = "testDataSource",
                server = container.containerIpAddress,
                port = container.firstMappedPort,
                username = "na",
                password = "na",
                database = "na"
        )

        val cluster = getCluster(dataSource)

        var client = cluster.connect<Client>().init()

        client.submit("graph.io(graphml()).readGraph('data/grateful-dead.xml')")

        client.close()
    }

}