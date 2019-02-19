package com.arcadeanalytics.provider.orientdb

import com.arcadeanalytics.provider.orient2.OrientDBDataSourceGraphDataProvider
import com.arcadeanalytics.provider.orientdb.OrientDBContainer.dataSource
import com.arcadeanalytics.provider.orientdb.OrientDBContainer.dbUrl
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class OrientDBDataSourceGraphDataProviderIntTest {

    private val provider: OrientDBDataSourceGraphDataProvider = OrientDBDataSourceGraphDataProvider()

    @Test
    @Throws(Exception::class)
    fun shouldFetchDataWithQuery() {

        val query = "select from Person limit 20"
        val data = provider.fetchData(OrientDBContainer.dataSource, query, 20)


        //then
        assertThat(data.nodes).hasSize(4)

        assertThat(data.nodesClasses).containsKeys("Person")

        val cytoData = data.nodes.first()
        assertThat(cytoData.group).isEqualTo("nodes")
        assertThat(cytoData.data.source).isEmpty()

        val record = cytoData.data.record
        assertThat(record).isNotNull
                .containsKeys("name", "@out", "@in", "@edgeCount")

    }


    @Test
    @Throws(Exception::class)
    fun shouldTraverseFromGivenNode() {
        //given


        //        final String rid = getFirstPersonIdentity();

        val ids = getPersonsIdentity(2)
        //when
        val data = provider.expand(dataSource, ids, "out", "FriendOf", 300)

        //then
        assertThat(data.nodes).hasSize(2)
        assertThat(data.edges).hasSize(1)

        assertThat(data.nodesClasses).containsKeys("Person")
        assertThat(data.edgesClasses).containsKeys("FriendOf")

        val cytoData = data.nodes.stream().findFirst().get()
        assertThat(cytoData.data.record).isNotNull
        assertThat(cytoData.data.source).isEmpty()

    }

    @Test
    @Throws(Exception::class)
    fun shouldTraverseAllEdgesNode() {
        //given


        //        final String rid = getFirstPersonIdentity();

        val ids = getPersonsIdentity(2)
        //when
        val data = provider.expand(dataSource, ids, "both", "", 300)

        //then
        assertThat(data.nodes).hasSize(4)
        assertThat(data.edges).hasSize(3)

        assertThat(data.nodesClasses).containsKeys("Person")
        assertThat(data.edgesClasses).containsKeys("FriendOf")
        assertThat(data.edgesClasses).containsKeys("HaterOf")

        val cytoData = data.nodes.stream().findFirst().get()
        assertThat(cytoData.data.record).isNotNull
        assertThat(cytoData.data.source).isEmpty()

    }

    @Test
    @Throws(Exception::class)
    fun shouldLoadGivenIds() {
        //given
        val ids = getPersonsIdentity(2)
        //when

        println("ids = " + ids)
        val data = provider.load(dataSource, ids)

        //then
        assertThat(data.nodes).hasSize(2)


        val cytoData = data.nodes.stream().findFirst().get()
        assertThat(cytoData.data.record).isNotNull
        assertThat(cytoData.data.source).isEmpty()

    }

    @Test
    fun shouldLoadFromClass() {
        val data = provider.loadFromClass(dataSource, "Person", 1)

        assertThat(data.nodes).hasSize(1)


    }

    @Test
    fun shouldLoadFromClassWherePropertyHasValue() {
        val data = provider.loadFromClass(dataSource, "Person", "name", "frank", 10)

        assertThat(data.nodes).hasSize(1)


    }

    private fun getPersonsIdentity(limit: Int): Array<String> {
        ODatabaseDocumentTx(dbUrl).open<ODatabaseDocumentTx>("admin", "admin")
                .use {

                    return it.query<List<ODocument>>(OSQLSynchQuery<ODocument>("""SELECT from Person limit $limit"""))
                            .asSequence()
                            .map { doc -> doc.identity }
                            .map { id -> id.clusterId.toString() + "_" + id.clusterPosition }
                            .toList()
                            .toTypedArray()

                }

    }

}
