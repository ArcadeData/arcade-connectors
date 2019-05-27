package com.arcadeanalytics.provider.rdbms

import com.arcadeanalytics.provider.DataSourceInfo
import com.arcadeanalytics.provider.rdbms.dataprovider.PostgreSQLContainerHolder
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer

internal class RDBMSTableDataProviderTest {

    private val container: PostgreSQLContainer<Nothing> = PostgreSQLContainerHolder.container as PostgreSQLContainer<Nothing>

    private lateinit var provider: RDBMSTableDataProvider

    private lateinit var dataSourceInfo: DataSourceInfo

    @BeforeEach
    @Throws(Exception::class)
    fun setUp() {


        dataSourceInfo = DataSourceInfo(id = 1L,
                type = "RDBMS_POSTGRESQL",
                name = "testDataSource",
                server = container.containerIpAddress,
                port = container.firstMappedPort,
                username = container.username,
                password = container.password,
                database = container.databaseName,
                aggregationEnabled = true
        )

        provider = RDBMSTableDataProvider()

    }

    @Test
    fun fetchData() {


        val data = provider.fetchData(dataSourceInfo, """SELECT customer_id, SUM (amount) total_amount
            | FROM  payment
            | GROUP BY customer_id
            | ORDER BY total_amount DESC
            | """.trimMargin(), 100)

        data.nodes.forEach { n -> println("n = ${n}") }

    }
}