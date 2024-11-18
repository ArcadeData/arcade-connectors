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

import com.arcadeanalytics.provider.QueryParam
import com.arcadeanalytics.provider.QueryParams
import com.arcadeanalytics.provider.TABLE_CLASS
import com.arcadeanalytics.provider.TypeProperties
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

internal class OrientDB3DataSourceTableDataProviderTest {
    private val provider: OrientDB3DataSourceTableDataProvider = OrientDB3DataSourceTableDataProvider()

    @Test
    @Throws(Exception::class)
    fun shouldFetchDataWithAggregateQuery() {
        // when

        val query = "select avg(age) as age, count(name) as count from Person group by age order by count desc"

        val data = provider.fetchData(OrientDB3Container.dataSource, query, 20)

        println("data = $data")
        // then
        assertThat(data.nodes).hasSize(3)

        assertThat(data.edges).hasSize(0)

        assertThat(data.nodesClasses)
            .containsOnlyKeys(TABLE_CLASS)

        val tableClass = data.nodesClasses[TABLE_CLASS]

        assertThat(tableClass).containsKeys("name", "cardinality", "properties")

        val properties: TypeProperties = tableClass?.get("properties") as TypeProperties

        assertThat(properties).containsKeys("count", "age")

        val cytoData = data.nodes.first()
        assertThat(cytoData.classes).isEqualTo(TABLE_CLASS)

        assertThat(cytoData.group)
            .isEqualTo("nodes")

        assertThat(cytoData.data.id).isEqualTo("0")
        assertThat(cytoData.data.source).isEmpty()
        assertThat(cytoData.data.target).isEmpty()

        val record = cytoData.data.record

        assertThat(record).containsOnlyKeys("count", "age")
    }

    @Test
    @Throws(Exception::class)
    @Disabled
    fun shouldFetchDataWithAggregateGremlinQuery() {
        // when

        val query = "gremlin: select avg(age) as age, count(name) as count from Person group by age order by count desc"

        val data = provider.fetchData(OrientDB3Container.dataSource, query, 20)

        println("data = $data")
        // then
        assertThat(data.nodes).hasSize(3)

        assertThat(data.edges).hasSize(0)

        assertThat(data.nodesClasses)
            .containsOnlyKeys(TABLE_CLASS)

        val tableClass = data.nodesClasses[TABLE_CLASS]

        assertThat(tableClass).containsKeys("name", "cardinality", "properties")

        val properties: TypeProperties = tableClass?.get("properties") as TypeProperties

        assertThat(properties).containsKeys("count", "age")

        val cytoData = data.nodes.first()
        assertThat(cytoData.classes).isEqualTo(TABLE_CLASS)

        assertThat(cytoData.group)
            .isEqualTo("nodes")

        assertThat(cytoData.data.id).isEqualTo("0")
        assertThat(cytoData.data.source).isEmpty()
        assertThat(cytoData.data.target).isEmpty()

        val record = cytoData.data.record

        assertThat(record).containsOnlyKeys("count", "age")
    }

    @Test
    @Throws(Exception::class)
    fun shouldFetchDataWithAggregateParametrizedQuery() {
        // when

        val query =
            """
            select avg(age) as age, count(name) as count
            from Person
            where age < :age
            group by age
            order by count desc
            limit :limit
            """.trimIndent()

        val params: QueryParams =
            listOf(
                QueryParam("age", "query", "(SELECT age FROM Person WHERE name ='rob')"),
                QueryParam("limit", "single", "1"),
            )

        val data = provider.fetchData(OrientDB3Container.dataSource, query, params, 20)

        println("data = $data")
        // then
        assertThat(data.nodes).hasSize(1)

        assertThat(data.edges).hasSize(0)

        assertThat(data.nodesClasses)
            .containsOnlyKeys(TABLE_CLASS)

        val tableClass = data.nodesClasses[TABLE_CLASS]

        assertThat(tableClass).containsKeys("name", "cardinality", "properties")

        val get: TypeProperties = tableClass?.get("properties") as TypeProperties

        assertThat(get).containsKeys("count", "age")

        val cytoData = data.nodes.first()
        assertThat(cytoData.classes).isEqualTo(TABLE_CLASS)

        assertThat(cytoData.group)
            .isEqualTo("nodes")

        assertThat(cytoData.data.id).isEqualTo("0")
        assertThat(cytoData.data.source).isEmpty()
        assertThat(cytoData.data.target).isEmpty()

        val record = cytoData.data.record

        assertThat(record).containsOnlyKeys("count", "age")
    }

    @Test
    @Throws(Exception::class)
    fun shouldFetchDataWithQuery() {
        // when

        val query = "select from Person "
        val data = provider.fetchData(OrientDB3Container.dataSource, query, 20)

        // then
        assertThat(data.nodes).hasSize(4)

        assertThat(data.edges).hasSize(0)

        assertThat(data.nodesClasses)
            .containsOnlyKeys(TABLE_CLASS)

        val tableClass = data.nodesClasses[TABLE_CLASS]
        assertThat(tableClass).containsKeys("name", "cardinality", "properties")

        val get: TypeProperties = tableClass?.get("properties") as TypeProperties

        assertThat(get).containsKeys("name", "age")

//        assertThat(tableClass).contains(entry("name", "String"), entry("age", "Numeric"))

        val cytoData = data.nodes.first()
        assertThat(cytoData.classes).isEqualTo(TABLE_CLASS)
        assertThat(cytoData.group)
            .isEqualTo("nodes")

        assertThat(cytoData.data.id).isEqualTo("0")
        assertThat(cytoData.data.source).isEmpty()
        assertThat(cytoData.data.target).isEmpty()

        val record = cytoData.data.record
        assertThat(record).containsOnlyKeys("name", "age")
    }
}
