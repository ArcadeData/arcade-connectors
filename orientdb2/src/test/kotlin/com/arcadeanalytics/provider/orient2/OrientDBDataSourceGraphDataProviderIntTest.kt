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
package com.arcadeanalytics.provider.orientdb

import com.arcadeanalytics.provider.orient2.OrientDBDataSourceGraphDataProvider
import com.arcadeanalytics.provider.orient2.OrientDBContainer.dataSource
import com.arcadeanalytics.provider.orient2.OrientDBContainer.dbUrl
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
        val data = provider.fetchData(dataSource, query, 20)


        //then
        assertThat(data.nodes).hasSize(4)

        assertThat(data.nodesClasses).containsKeys("Person")

        val cytoData = data.nodes.first()
        assertThat(cytoData.group).isEqualTo("nodes")
        assertThat(cytoData.data.source).isEmpty()

        assertThat(cytoData.data.id).startsWith("${dataSource.id}")
        val record = cytoData.data.record
        assertThat(record).isNotNull
                .containsKeys("name", "@out", "@in", "@edgeCount")

    }


    @Test
    @Throws(Exception::class)
    fun shouldTraverseFromGivenNode() {
        //given
        val person = provider.loadFromClass(dataSource, "Person", "name", "frank", 1)

        val id = person.nodes.first().data.id

        //when
        val data = provider.expand(dataSource, arrayOf(id), "in", "FriendOf", 300)

        //then
        assertThat(data.nodes).hasSize(2)
        assertThat(data.edges).hasSize(1)

        assertThat(data.nodesClasses).containsKeys("Person")
        assertThat(data.edgesClasses).containsKeys("FriendOf")

        val cytoData = data.nodes.first()
        val record = cytoData.data.record
        assertThat(record).isNotNull
        assertThat(cytoData.data.source).isEmpty()

    }

    @Test
    @Throws(Exception::class)
    fun shouldTraverseAllEdgesNode() {
        //given

        val person = provider.loadFromClass(dataSource, "Person", "name", "frank", 1)

        val id = person.nodes.first().data.id

        //when
        val data = provider.expand(dataSource, arrayOf(id), "both", "", 300)

        println("data = ${data}")
        //then
        assertThat(data.nodes).hasSize(3)
        assertThat(data.edges).hasSize(2)

        assertThat(data.nodesClasses).containsKeys("Person")
        assertThat(data.edgesClasses).containsKeys("FriendOf")
        assertThat(data.edgesClasses).containsKeys("HaterOf")

        val cytoData = data.nodes.first()
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
