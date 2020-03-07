/*-
 * #%L
 * Arcade Connectors
 * %%
 * Copyright (C) 2018 - 2020 ArcadeData
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
package com.arcadeanalytics.provider.neo

import com.arcadeanalytics.provider.DataSourceInfo
import com.arcadeanalytics.provider.neo4j3.getDriver
import com.arcadeanalytics.test.KGenericContainer
import org.neo4j.driver.v1.Session
import org.testcontainers.containers.wait.strategy.Wait


object Neo4jContainer {

    private val container: KGenericContainer = KGenericContainer("neo4j:latest")
            .apply {
                withExposedPorts(7687, 7474)
                withEnv("NEO4J_AUTH", "neo4j/arcade")
                waitingFor(Wait.forListeningPort())
                start()

            }

    val dataSource: DataSourceInfo

    init {
        dataSource = DataSourceInfo(id = 1L,
                type = "NEO4J",
                name = "testDataSource",
                server = container.containerIpAddress,
                port = container.firstMappedPort,
                username = "neo4j",
                password = "arcade",
                database = "empty"
        )
        getDriver(dataSource).use { driver ->

            driver.session().use {
                fillDatabase(it)
            }
        }
    }
}

fun fillDatabase(session: Session) {

    session.run("CREATE (a:Person {name: 'rob'})")

    session.run("CREATE (a:Person {name: 'frank'})")

    session.run("CREATE (a:Person {name: 'john'})")

    session.run("CREATE (a:Person {name: 'jane'})")


    session.run("""MATCH (a:Person),(b:Person)
                WHERE a.name = 'rob' AND b.name = 'frank'
                CREATE (a)-[r:FriendOf { kind: 'fraternal' }]->(b)
                RETURN r""")
    session.run("""MATCH (a:Person),(b:Person)
                WHERE a.name = 'john' AND b.name = 'jane'
                CREATE (a)-[r:FriendOf { kind: 'fraternal' }]->(b)
                RETURN r""")

    session.run("""MATCH (a:Person),(b:Person)
                WHERE a.name = 'jane' AND b.name = 'rob'
                CREATE (a)-[r:HaterOf { kind: 'killer' }]->(b)
                RETURN r""")
    session.run("""MATCH (a:Person),(b:Person)
                WHERE a.name = 'frank' AND b.name = 'john'
                CREATE (a)-[r:HaterOf { kind: 'killer' }]->(b)
                RETURN r""")


    session.run("CREATE (a:Car {name: 'gt40'})")
    session.run("CREATE (a:Car {name: 'laferrari'})")
    session.run("CREATE (a:Car {name: 'huracan'})")
    session.run("CREATE (a:Car {name: 'f150'})")

}
