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
package com.arcadeanalytics.provider.rdbms

import com.arcadeanalytics.provider.DataSourceInfo
import com.arcadeanalytics.provider.rdbms.dataprovider.PostgreSQLContainerHolder
import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.PostgreSQLContainer

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
        dataSourceNoAggregation =
            DataSourceInfo(
                id = 1L,
                type = "RDBMS_POSTGRESQL",
                name = "testDataSource",
                server = container.containerIpAddress,
                port = container.firstMappedPort,
                username = container.username,
                password = container.password,
                database = container.databaseName,
                aggregationEnabled = false,
            )
        dataSourceWithAggregation =
            DataSourceInfo(
                id = 1L,
                type = "RDBMS_POSTGRESQL",
                name = "testDataSource",
                server = container.containerIpAddress,
                port = container.firstMappedPort,
                username = container.username,
                password = container.password,
                database = container.databaseName,
                aggregationEnabled = true,
            )

        providerNoAggregation = RDBMSMetadataProvider()
        providerWithAggregation = RDBMSMetadataProvider()
    }

    @Test
    fun shouldFetchMetadata() {
        val metadata = providerNoAggregation.fetchMetadata(dataSourceNoAggregation)

        assertThat(metadata.nodesClasses)
            .isNotEmpty
            .containsKeys("actor", "address", "category", "city", "country", "customer", "film", "rental", "payment", "language")

        assertThat(metadata.edgesClasses).isNotEmpty
    }

    @Test
    fun fetchMetadataWithoutAggregation() {
        val metadata = providerNoAggregation.fetchMetadata(dataSourceNoAggregation)

        println("metadata = $metadata")

        Assertions.assertThat(metadata.nodesClasses).isNotEmpty
        Assertions.assertThat(metadata.edgesClasses).isNotEmpty

        val nodesClasses = metadata.nodesClasses
        val edgesClasses = metadata.edgesClasses
        assertThat(nodesClasses.size).isEqualTo(15)
        assertThat(edgesClasses.size).isEqualTo(12)

        /*
         * Vertices Classes check
         */

        var currVertexClassName = "actor"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(200)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(4)
        }

        currVertexClassName = "address"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(603)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(8)
        }

        currVertexClassName = "category"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(16)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(3)
        }

        currVertexClassName = "city"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(600)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(4)
        }

        currVertexClassName = "country"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(109)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(3)
        }

        currVertexClassName = "customer"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(599)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(10)
        }

        currVertexClassName = "film"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(1000)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(13)
        }

        currVertexClassName = "film_actor"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(5462)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(3)
        }

        currVertexClassName = "film_category"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(1000)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(3)
        }

        currVertexClassName = "inventory"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(4581)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(4)
        }

        currVertexClassName = "language"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(6)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(3)
        }

        currVertexClassName = "payment"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(14596)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(6)
        }

        currVertexClassName = "rental"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(16044)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(7)
        }

        currVertexClassName = "staff"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(2)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(11)
        }

        currVertexClassName = "store"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(2)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(4)
        }

        /*
         * Edges Classes check
         */

        var currEdgeClassName = "has_actor"
        assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        assertThat(edgesClasses.get(currEdgeClassName)).isNotNull()
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currEdgeClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(5462)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(0)
        }

        currEdgeClassName = "has_address"
        assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        assertThat(edgesClasses.get(currEdgeClassName)).isNotNull()
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currEdgeClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(603)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(0)
        }

        currEdgeClassName = "has_category"
        assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        assertThat(edgesClasses.get(currEdgeClassName)).isNotNull()
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currEdgeClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(1000)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(0)
        }

        currEdgeClassName = "has_city"
        assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        assertThat(edgesClasses.get(currEdgeClassName)).isNotNull()
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currEdgeClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(603)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(0)
        }

        currEdgeClassName = "has_country"
        assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        assertThat(edgesClasses.get(currEdgeClassName)).isNotNull()
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currEdgeClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(600)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(0)
        }

        currEdgeClassName = "has_customer"
        assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        assertThat(edgesClasses.get(currEdgeClassName)).isNotNull()
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currEdgeClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(30640)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(0)
        }

        currEdgeClassName = "has_film"
        assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        assertThat(edgesClasses.get(currEdgeClassName)).isNotNull()
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currEdgeClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(11043)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(0)
        }

        currEdgeClassName = "has_inventory"
        assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        assertThat(edgesClasses.get(currEdgeClassName)).isNotNull()
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currEdgeClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(16044)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(0)
        }

        currEdgeClassName = "has_language"
        assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        assertThat(edgesClasses.get(currEdgeClassName)).isNotNull()
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currEdgeClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(1000)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(0)
        }

        currEdgeClassName = "has_manager_staff"
        assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        assertThat(edgesClasses.get(currEdgeClassName)).isNotNull()
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currEdgeClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(2)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(0)
        }

        currEdgeClassName = "has_rental"
        assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        assertThat(edgesClasses.get(currEdgeClassName)).isNotNull()
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currEdgeClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(14596)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(0)
        }

        currEdgeClassName = "has_staff"
        assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        assertThat(edgesClasses.get(currEdgeClassName)).isNotNull()
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currEdgeClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(30640)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(0)
        }
    }

    @Test
    fun fetchMetadataWithAggregation() {
        val metadata = providerWithAggregation.fetchMetadata(dataSourceWithAggregation)

        println("metadata = $metadata")

        Assertions.assertThat(metadata.nodesClasses).isNotEmpty
        Assertions.assertThat(metadata.edgesClasses).isNotEmpty

        val nodesClasses = metadata.nodesClasses
        val edgesClasses = metadata.edgesClasses
        assertThat(nodesClasses.size).isEqualTo(13)
        assertThat(edgesClasses.size).isEqualTo(12)

        /*
         * Vertices Classes check
         */

        var currVertexClassName = "actor"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(200)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(4)
        }

        currVertexClassName = "address"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(603)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(8)
        }

        currVertexClassName = "category"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(16)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(3)
        }

        currVertexClassName = "city"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(600)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(4)
        }

        currVertexClassName = "country"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(109)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(3)
        }

        currVertexClassName = "customer"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(599)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(10)
        }

        currVertexClassName = "film"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(1000)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(13)
        }

        currVertexClassName = "inventory"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(4581)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(4)
        }

        currVertexClassName = "language"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(6)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(3)
        }

        currVertexClassName = "payment"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(14596)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(6)
        }

        currVertexClassName = "rental"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(16044)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(7)
        }

        currVertexClassName = "staff"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(2)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(11)
        }

        currVertexClassName = "store"
        assertTrue(nodesClasses.keys.contains(currVertexClassName))
        assertThat(nodesClasses.get(currVertexClassName)).isNotNull()
        nodesClasses.get(currVertexClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currVertexClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(2)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(4)
        }

        /*
         * Edges Classes check
         */

        var currEdgeClassName = "film_actor"
        assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        assertThat(edgesClasses.get(currEdgeClassName)).isNotNull()
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currEdgeClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(5462)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(1)
        }

        currEdgeClassName = "film_category"
        assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        assertThat(edgesClasses.get(currEdgeClassName)).isNotNull()
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currEdgeClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(1000)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(1)
        }

        currEdgeClassName = "has_address"
        assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        assertThat(edgesClasses.get(currEdgeClassName)).isNotNull()
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currEdgeClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(603)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(0)
        }

        currEdgeClassName = "has_city"
        assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        assertThat(edgesClasses.get(currEdgeClassName)).isNotNull()
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currEdgeClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(603)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(0)
        }

        currEdgeClassName = "has_country"
        assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        assertThat(edgesClasses.get(currEdgeClassName)).isNotNull()
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currEdgeClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(600)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(0)
        }

        currEdgeClassName = "has_customer"
        assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        assertThat(edgesClasses.get(currEdgeClassName)).isNotNull()
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currEdgeClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(30640)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(0)
        }

        currEdgeClassName = "has_film"
        assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        assertThat(edgesClasses.get(currEdgeClassName)).isNotNull()
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currEdgeClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(4581)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(0)
        }

        currEdgeClassName = "has_inventory"
        assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        assertThat(edgesClasses.get(currEdgeClassName)).isNotNull()
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currEdgeClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(16044)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(0)
        }

        currEdgeClassName = "has_language"
        assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        assertThat(edgesClasses.get(currEdgeClassName)).isNotNull()
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currEdgeClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(1000)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(0)
        }

        currEdgeClassName = "has_manager_staff"
        assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        assertThat(edgesClasses.get(currEdgeClassName)).isNotNull()
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currEdgeClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(2)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(0)
        }

        currEdgeClassName = "has_rental"
        assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        assertThat(edgesClasses.get(currEdgeClassName)).isNotNull()
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currEdgeClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(14596)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(0)
        }

        currEdgeClassName = "has_staff"
        assertTrue(edgesClasses.keys.contains(currEdgeClassName))
        assertThat(edgesClasses.get(currEdgeClassName)).isNotNull()
        edgesClasses.get(currEdgeClassName)?.let { currVertexClassInfo ->
            assertThat(currVertexClassInfo.name).isEqualTo(currEdgeClassName)
            assertThat(currVertexClassInfo.cardinality).isEqualTo(30640)
            assertThat(currVertexClassInfo.properties.size).isEqualTo(0)
        }
    }
}
