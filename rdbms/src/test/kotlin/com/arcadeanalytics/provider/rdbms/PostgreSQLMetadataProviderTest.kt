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
package com.arcadeanalytics.provider.rdbms

import com.arcadeanalytics.provider.DataSourceInfo
import com.arcadeanalytics.provider.rdbms.dataprovider.PostgreSQLContainerHolder
import org.assertj.core.api.Assertions
import org.junit.Assert
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.PostgreSQLContainer

//@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PostgreSQLMetadataProviderTest {

    private val container: PostgreSQLContainer<Nothing> = PostgreSQLContainerHolder.container as PostgreSQLContainer<Nothing>

    private lateinit var providerNoAggregation: RDBMSMetadataProvider
    private lateinit var providerWithAggregation: RDBMSMetadataProvider

    private lateinit var dataSourceNoAggregation: DataSourceInfo
    private lateinit var dataSourceWithAggregation: DataSourceInfo

    @BeforeEach
    @Throws(Exception::class)
    fun setUp() {


        dataSourceNoAggregation = DataSourceInfo(id = 1L,
                type = "RDBMS_POSTGRESQL",
                name = "testDataSource",
                server = container.containerIpAddress,
                port = container.firstMappedPort,
                username = container.username,
                password = container.password,
                database = container.databaseName,
                aggregationEnabled = false
        )
        dataSourceWithAggregation = DataSourceInfo(id = 1L,
                type = "RDBMS_POSTGRESQL",
                name = "testDataSource",
                server = container.containerIpAddress,
                port = container.firstMappedPort,
                username = container.username,
                password = container.password,
                database = container.databaseName,
                aggregationEnabled = true
        )

        providerNoAggregation = RDBMSMetadataProvider()
        providerWithAggregation = RDBMSMetadataProvider()

    }

    @Test
    fun shouldFetchMetadata() {


        val metadata = providerNoAggregation.fetchMetadata(dataSourceNoAggregation)

        println("metadata = ${metadata}")

        Assertions.assertThat(metadata.nodesClasses).isNotEmpty
        Assertions.assertThat(metadata.edgesClasses).isNotEmpty
    }

    @Test
    fun fetchMetadataWithoutAggregation() {


        val metadata = providerNoAggregation.fetchMetadata(dataSourceNoAggregation)

        println("metadata = ${metadata}")

        Assertions.assertThat(metadata.nodesClasses).isNotEmpty
        Assertions.assertThat(metadata.edgesClasses).isNotEmpty

        val nodesClasses = metadata.nodesClasses
        val edgesClasses = metadata.edgesClasses
        Assert.assertEquals(15, nodesClasses.size)
        Assert.assertEquals(12, edgesClasses.size)

        /*
         * Vertices Classes check
         */

        var currVertexClassName = "actor"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(200, currVertexClassInfo.cardinality)
            Assert.assertEquals(4, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "address"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(603, currVertexClassInfo.cardinality)
            Assert.assertEquals(8, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "category"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(16, currVertexClassInfo.cardinality)
            Assert.assertEquals(3, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "city"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(600, currVertexClassInfo.cardinality)
            Assert.assertEquals(4, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "country"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(109, currVertexClassInfo.cardinality)
            Assert.assertEquals(3, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "customer"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(599, currVertexClassInfo.cardinality)
            Assert.assertEquals(10, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "film"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(1000, currVertexClassInfo.cardinality)
            Assert.assertEquals(13, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "film_actor"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(5462, currVertexClassInfo.cardinality)
            Assert.assertEquals(3, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "film_category"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(1000, currVertexClassInfo.cardinality)
            Assert.assertEquals(3, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "inventory"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(4581, currVertexClassInfo.cardinality)
            Assert.assertEquals(4, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "language"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(6, currVertexClassInfo.cardinality)
            Assert.assertEquals(3, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "payment"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(14596, currVertexClassInfo.cardinality)
            Assert.assertEquals(6, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "rental"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(16044, currVertexClassInfo.cardinality)
            Assert.assertEquals(7, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "staff"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(2, currVertexClassInfo.cardinality)
            Assert.assertEquals(11, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "store"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(2, currVertexClassInfo.cardinality)
            Assert.assertEquals(4, currVertexClassInfo.properties.size)
        }

        /*
         * Edges Classes check
         */

        var currEdgeClassName = "has_actor"
        Assert.assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        Assert.assertNotNull(edgesClasses.get(currEdgeClassName))
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currEdgeClassName, currVertexClassInfo.name)
            Assert.assertEquals(5462, currVertexClassInfo.cardinality)
            Assert.assertEquals(0, currVertexClassInfo.properties.size)
        }

        currEdgeClassName = "has_address"
        Assert.assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        Assert.assertNotNull(edgesClasses.get(currEdgeClassName))
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currEdgeClassName, currVertexClassInfo.name)
            Assert.assertEquals(603, currVertexClassInfo.cardinality)
            Assert.assertEquals(0, currVertexClassInfo.properties.size)
        }

        currEdgeClassName = "has_category"
        Assert.assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        Assert.assertNotNull(edgesClasses.get(currEdgeClassName))
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currEdgeClassName, currVertexClassInfo.name)
            Assert.assertEquals(1000, currVertexClassInfo.cardinality)
            Assert.assertEquals(0, currVertexClassInfo.properties.size)
        }

        currEdgeClassName = "has_city"
        Assert.assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        Assert.assertNotNull(edgesClasses.get(currEdgeClassName))
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currEdgeClassName, currVertexClassInfo.name)
            Assert.assertEquals(603, currVertexClassInfo.cardinality)
            Assert.assertEquals(0, currVertexClassInfo.properties.size)
        }

        currEdgeClassName = "has_country"
        Assert.assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        Assert.assertNotNull(edgesClasses.get(currEdgeClassName))
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currEdgeClassName, currVertexClassInfo.name)
            Assert.assertEquals(600, currVertexClassInfo.cardinality)
            Assert.assertEquals(0, currVertexClassInfo.properties.size)
        }

        currEdgeClassName = "has_customer"
        Assert.assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        Assert.assertNotNull(edgesClasses.get(currEdgeClassName))
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currEdgeClassName, currVertexClassInfo.name)
            Assert.assertEquals(30640, currVertexClassInfo.cardinality)
            Assert.assertEquals(0, currVertexClassInfo.properties.size)
        }

        currEdgeClassName = "has_film"
        Assert.assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        Assert.assertNotNull(edgesClasses.get(currEdgeClassName))
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currEdgeClassName, currVertexClassInfo.name)
            Assert.assertEquals(11043, currVertexClassInfo.cardinality)
            Assert.assertEquals(0, currVertexClassInfo.properties.size)
        }

        currEdgeClassName = "has_inventory"
        Assert.assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        Assert.assertNotNull(edgesClasses.get(currEdgeClassName))
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currEdgeClassName, currVertexClassInfo.name)
            Assert.assertEquals(16044, currVertexClassInfo.cardinality)
            Assert.assertEquals(0, currVertexClassInfo.properties.size)
        }

        currEdgeClassName = "has_language"
        Assert.assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        Assert.assertNotNull(edgesClasses.get(currEdgeClassName))
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currEdgeClassName, currVertexClassInfo.name)
            Assert.assertEquals(1000, currVertexClassInfo.cardinality)
            Assert.assertEquals(0, currVertexClassInfo.properties.size)
        }

        currEdgeClassName = "has_manager_staff"
        Assert.assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        Assert.assertNotNull(edgesClasses.get(currEdgeClassName))
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currEdgeClassName, currVertexClassInfo.name)
            Assert.assertEquals(2, currVertexClassInfo.cardinality)
            Assert.assertEquals(0, currVertexClassInfo.properties.size)
        }

        currEdgeClassName = "has_rental"
        Assert.assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        Assert.assertNotNull(edgesClasses.get(currEdgeClassName))
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currEdgeClassName, currVertexClassInfo.name)
            Assert.assertEquals(14596, currVertexClassInfo.cardinality)
            Assert.assertEquals(0, currVertexClassInfo.properties.size)
        }

        currEdgeClassName = "has_staff"
        Assert.assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        Assert.assertNotNull(edgesClasses.get(currEdgeClassName))
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currEdgeClassName, currVertexClassInfo.name)
            Assert.assertEquals(30640, currVertexClassInfo.cardinality)
            Assert.assertEquals(0, currVertexClassInfo.properties.size)
        }
    }


    @Test
    fun fetchMetadataWithAggregation() {


        val metadata = providerWithAggregation.fetchMetadata(dataSourceWithAggregation)

        println("metadata = ${metadata}")

        Assertions.assertThat(metadata.nodesClasses).isNotEmpty
        Assertions.assertThat(metadata.edgesClasses).isNotEmpty

        val nodesClasses = metadata.nodesClasses
        val edgesClasses = metadata.edgesClasses
        Assert.assertEquals(13, nodesClasses.size)
        Assert.assertEquals(12, edgesClasses.size)

        /*
         * Vertices Classes check
         */

        var currVertexClassName = "actor"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(200, currVertexClassInfo.cardinality)
            Assert.assertEquals(4, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "address"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(603, currVertexClassInfo.cardinality)
            Assert.assertEquals(8, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "category"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(16, currVertexClassInfo.cardinality)
            Assert.assertEquals(3, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "city"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(600, currVertexClassInfo.cardinality)
            Assert.assertEquals(4, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "country"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(109, currVertexClassInfo.cardinality)
            Assert.assertEquals(3, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "customer"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(599, currVertexClassInfo.cardinality)
            Assert.assertEquals(10, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "film"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(1000, currVertexClassInfo.cardinality)
            Assert.assertEquals(13, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "inventory"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(4581, currVertexClassInfo.cardinality)
            Assert.assertEquals(4, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "language"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(6, currVertexClassInfo.cardinality)
            Assert.assertEquals(3, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "payment"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(14596, currVertexClassInfo.cardinality)
            Assert.assertEquals(6, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "rental"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(16044, currVertexClassInfo.cardinality)
            Assert.assertEquals(7, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "staff"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(2, currVertexClassInfo.cardinality)
            Assert.assertEquals(11, currVertexClassInfo.properties.size)
        }

        currVertexClassName = "store"
        Assert.assertTrue(nodesClasses.keys.contains(currVertexClassName))
        Assert.assertNotNull(nodesClasses.get(currVertexClassName))
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currVertexClassName, currVertexClassInfo.name)
            Assert.assertEquals(2, currVertexClassInfo.cardinality)
            Assert.assertEquals(4, currVertexClassInfo.properties.size)
        }

        /*
         * Edges Classes check
         */

        var currEdgeClassName = "film_actor"
        Assert.assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        Assert.assertNotNull(edgesClasses.get(currEdgeClassName))
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currEdgeClassName, currVertexClassInfo.name)
            Assert.assertEquals(5462, currVertexClassInfo.cardinality)
            Assert.assertEquals(1, currVertexClassInfo.properties.size)
        }

        currEdgeClassName = "film_category"
        Assert.assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        Assert.assertNotNull(edgesClasses.get(currEdgeClassName))
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currEdgeClassName, currVertexClassInfo.name)
            Assert.assertEquals(1000, currVertexClassInfo.cardinality)
            Assert.assertEquals(1, currVertexClassInfo.properties.size)
        }

        currEdgeClassName = "has_address"
        Assert.assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        Assert.assertNotNull(edgesClasses.get(currEdgeClassName))
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currEdgeClassName, currVertexClassInfo.name)
            Assert.assertEquals(603, currVertexClassInfo.cardinality)
            Assert.assertEquals(0, currVertexClassInfo.properties.size)
        }

        currEdgeClassName = "has_city"
        Assert.assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        Assert.assertNotNull(edgesClasses.get(currEdgeClassName))
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currEdgeClassName, currVertexClassInfo.name)
            Assert.assertEquals(603, currVertexClassInfo.cardinality)
            Assert.assertEquals(0, currVertexClassInfo.properties.size)
        }

        currEdgeClassName = "has_country"
        Assert.assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        Assert.assertNotNull(edgesClasses.get(currEdgeClassName))
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currEdgeClassName, currVertexClassInfo.name)
            Assert.assertEquals(600, currVertexClassInfo.cardinality)
            Assert.assertEquals(0, currVertexClassInfo.properties.size)
        }

        currEdgeClassName = "has_customer"
        Assert.assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        Assert.assertNotNull(edgesClasses.get(currEdgeClassName))
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currEdgeClassName, currVertexClassInfo.name)
            Assert.assertEquals(30640, currVertexClassInfo.cardinality)
            Assert.assertEquals(0, currVertexClassInfo.properties.size)
        }

        currEdgeClassName = "has_film"
        Assert.assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        Assert.assertNotNull(edgesClasses.get(currEdgeClassName))
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currEdgeClassName, currVertexClassInfo.name)
            Assert.assertEquals(4581, currVertexClassInfo.cardinality)
            Assert.assertEquals(0, currVertexClassInfo.properties.size)
        }

        currEdgeClassName = "has_inventory"
        Assert.assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        Assert.assertNotNull(edgesClasses.get(currEdgeClassName))
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currEdgeClassName, currVertexClassInfo.name)
            Assert.assertEquals(16044, currVertexClassInfo.cardinality)
            Assert.assertEquals(0, currVertexClassInfo.properties.size)
        }

        currEdgeClassName = "has_language"
        Assert.assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        Assert.assertNotNull(edgesClasses.get(currEdgeClassName))
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currEdgeClassName, currVertexClassInfo.name)
            Assert.assertEquals(1000, currVertexClassInfo.cardinality)
            Assert.assertEquals(0, currVertexClassInfo.properties.size)
        }

        currEdgeClassName = "has_manager_staff"
        Assert.assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        Assert.assertNotNull(edgesClasses.get(currEdgeClassName))
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currEdgeClassName, currVertexClassInfo.name)
            Assert.assertEquals(2, currVertexClassInfo.cardinality)
            Assert.assertEquals(0, currVertexClassInfo.properties.size)
        }

        currEdgeClassName = "has_rental"
        Assert.assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        Assert.assertNotNull(edgesClasses.get(currEdgeClassName))
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currEdgeClassName, currVertexClassInfo.name)
            Assert.assertEquals(14596, currVertexClassInfo.cardinality)
            Assert.assertEquals(0, currVertexClassInfo.properties.size)
        }

        currEdgeClassName = "has_staff"
        Assert.assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        Assert.assertNotNull(edgesClasses.get(currEdgeClassName))
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            Assert.assertEquals(currEdgeClassName, currVertexClassInfo.name)
            Assert.assertEquals(30640, currVertexClassInfo.cardinality)
            Assert.assertEquals(0, currVertexClassInfo.properties.size)
        }
    }
}
