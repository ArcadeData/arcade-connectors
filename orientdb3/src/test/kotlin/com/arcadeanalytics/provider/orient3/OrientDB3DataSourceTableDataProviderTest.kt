package com.arcadeanalytics.provider.orient3

import com.arcadeanalytics.provider.QueryParam
import com.arcadeanalytics.provider.QueryParams
import com.arcadeanalytics.provider.TABLE_CLASS
import com.arcadeanalytics.provider.TypeProperties
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class OrientDB3DataSourceTableDataProviderTest {

    private val provider: OrientDB3DataSourceTableDataProvider = OrientDB3DataSourceTableDataProvider()


    @Test
    @Throws(Exception::class)
    fun shouldFetchDataWithAggregateQuery() {

        //when

        val query = "select avg(age) as age, count(name) as count from Person group by age order by count desc"

        val data = provider.fetchData(OrientDB3Container.dataSource, query, 20)

        println("data = ${data}")
        //then
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
    fun shouldFetchDataWithAggregateGremlinQuery() {

        //when

        val query = "gremlin: select avg(age) as age, count(name) as count from Person group by age order by count desc"

        val data = provider.fetchData(OrientDB3Container.dataSource, query, 20)

        println("data = ${data}")
        //then
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

        //when

        var query = "select avg(age) as age, count(name) as count from Person  where age < :age   group by age order by count desc limit :limit "

        val params: QueryParams = listOf(
                QueryParam("age", "query", "(SELECT age FROM Person WHERE name ='rob')"),
                QueryParam("limit", "single", "1"))

        val data = provider.fetchData(OrientDB3Container.dataSource, query, params, 20)

        println("data = ${data}")
        //then
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

        //when

        val query = "select from Person "
        val data = provider.fetchData(OrientDB3Container.dataSource, query, 20)

        //then
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