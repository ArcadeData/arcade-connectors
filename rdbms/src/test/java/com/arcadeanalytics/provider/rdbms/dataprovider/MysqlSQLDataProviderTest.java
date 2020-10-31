package com.arcadeanalytics.provider.rdbms.dataprovider;

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

import com.arcadeanalytics.provider.CytoData;
import com.arcadeanalytics.provider.DataSourceInfo;
import com.arcadeanalytics.provider.GraphData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;


public class MysqlSQLDataProviderTest extends AbstractRDBMSProviderTest {

    private static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssX");


    private DataSourceInfo dataSource = null;

    @BeforeEach
    public void setUp() throws Exception {

        final MySQLContainer container = MySQLContainerHolder.container;
        this.dataSource = new DataSourceInfo(
                1L,
                "RDBMS_MYSQL",
                "testDataSource",
                "desc",
                container.getContainerIpAddress(),
                container.getFirstMappedPort(),
                container.getDatabaseName(),
                container.getUsername(),
                container.getPassword(),
                false,
                "{}",
                false,
                false,
                "",
                22,
                "",
                false
        );

        provider = new RDBMSDataProvider();
    }


    @Override
    @Test
    public void fetchDataThroughTableScanTest() throws Exception {

        String query = "select * from actor limit 5";

        GraphData data = provider.fetchData(dataSource, query, 5);

        assertEquals(data.getNodesClasses().size(), 1);
        assertEquals(data.getNodes().size(), 5);
        assertEquals(data.getEdgesClasses().size(), 0);
        assertEquals(data.getEdges().size(), 0);

        // Node classes checks
        assertTrue(data.getNodesClasses().containsKey("actor"));

        Map<String, Object> actorClass = data.getNodesClasses().get("actor");
        assertTrue(actorClass.containsKey("actor_id"));
        assertTrue(actorClass.containsKey("first_name"));
        assertTrue(actorClass.containsKey("last_name"));
        assertTrue(actorClass.containsKey("last_update"));
        // nodes checks
        Iterator<CytoData> it = data.getNodes().iterator();
        CytoData currNodeContent;
        Map<String, Object> currRecord;

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("actor_id"));
        assertEquals(1, currRecord.get("actor_id"));
        assertTrue(currRecord.containsKey("first_name"));
        assertEquals("PENELOPE", currRecord.get("first_name"));
        assertTrue(currRecord.containsKey("last_name"));
        assertEquals("GUINESS", currRecord.get("last_name"));
        assertTrue(currRecord.containsKey("last_update"));
//        assertEquals("2006-02-15 05:34:33.0", currRecord.get("last_update").toString());
        assertEquals(df.parse("2006-02-15 04:34:33-00").toInstant(), ((Date) currRecord.get("last_update")).toInstant());

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("actor_id"));
        assertEquals(currRecord.get("actor_id"), 2);
        assertTrue(currRecord.containsKey("first_name"));
        assertEquals(currRecord.get("first_name"), "NICK");
        assertTrue(currRecord.containsKey("last_name"));
        assertEquals(currRecord.get("last_name"), "WAHLBERG");
        assertTrue(currRecord.containsKey("last_update"));
//        assertEquals("2006-02-15 05:34:33.0", currRecord.get("last_update").toString());
        assertEquals(df.parse("2006-02-15 04:34:33-00").toInstant(), ((Date) currRecord.get("last_update")).toInstant());

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("actor_id"));
        assertEquals(3, currRecord.get("actor_id"));
        assertTrue(currRecord.containsKey("first_name"));
        assertEquals("ED", currRecord.get("first_name"));
        assertTrue(currRecord.containsKey("last_name"));
        assertEquals("CHASE", currRecord.get("last_name"));
        assertTrue(currRecord.containsKey("last_update"));
//        assertEquals("2006-02-15 05:34:33.0", currRecord.get("last_update").toString());
        assertEquals(df.parse("2006-02-15 04:34:33-00").toInstant(), ((Date) currRecord.get("last_update")).toInstant());

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("actor_id"));
        assertEquals(4, currRecord.get("actor_id"));
        assertTrue(currRecord.containsKey("first_name"));
        assertEquals("JENNIFER", currRecord.get("first_name"));
        assertTrue(currRecord.containsKey("last_name"));
        assertEquals("DAVIS", currRecord.get("last_name"));
        assertTrue(currRecord.containsKey("last_update"));
//        assertEquals("2006-02-15 05:34:33.0", currRecord.get("last_update").toString());
        assertEquals(df.parse("2006-02-15 04:34:33-00").toInstant(), ((Date) currRecord.get("last_update")).toInstant());

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("actor_id"));
        assertEquals(5, currRecord.get("actor_id"));
        assertTrue(currRecord.containsKey("first_name"));
        assertEquals("JOHNNY", currRecord.get("first_name"));
        assertTrue(currRecord.containsKey("last_name"));
        assertEquals("LOLLOBRIGIDA", currRecord.get("last_name"));
        assertTrue(currRecord.containsKey("last_update"));
//        assertEquals("2006-02-15 05:34:33.0", currRecord.get("last_update").toString());
        assertEquals(df.parse("2006-02-15 04:34:33-00").toInstant(), ((Date) currRecord.get("last_update")).toInstant());

    }

    @Override
    @Test
    public void loadVerticesFromIdsMultipleTablesTest() {

        /*
         * Fetching the first 5 vertices from the actor table and the first 2 vertices from the store table by cyto-ids
         * cyto-id from is x_y where:
         *  - x is refers to the source table (actor is the first table in the source db schema)
         *  - y is the external key, then the pk value of the source record
         */

        String[] ids = {"1_1", "1_2", "1_3", "1_4", "1_5", "16_1", "16_2"};

        GraphData data = provider.load(dataSource, ids);

        assertEquals(data.getNodesClasses().size(), 2);
        assertEquals(data.getNodes().size(), 7);
        assertEquals(data.getEdgesClasses().size(), 0);
        assertEquals(data.getEdges().size(), 0);

        // Node classes checks
        assertTrue(data.getNodesClasses().containsKey("actor"));
        assertTrue(data.getNodesClasses().containsKey("store"));

        Map<String, Object> actorClass = data.getNodesClasses().get("actor");
        assertTrue(actorClass.containsKey("actor_id"));
        assertTrue(actorClass.containsKey("first_name"));
        assertTrue(actorClass.containsKey("last_name"));
        assertTrue(actorClass.containsKey("last_update"));
        Map<String, Object> storeClass = data.getNodesClasses().get("store");
        assertTrue(storeClass.containsKey("store_id"));
        assertTrue(storeClass.containsKey("manager_staff_id"));
        assertTrue(storeClass.containsKey("address_id"));
        assertTrue(storeClass.containsKey("last_update"));

        // nodes checks
        Iterator<CytoData> it = data.getNodes().iterator();
        CytoData currNodeContent;
        Map<String, Object> currRecord;

        try {

            currNodeContent = it.next();
            assertNotNull(currNodeContent.getData());
            assertNotNull(currNodeContent.getData().getRecord());
            currRecord = currNodeContent.getData().getRecord();
            assertTrue(currRecord.containsKey("actor_id"));
            assertEquals(1, currRecord.get("actor_id"));
            assertTrue(currRecord.containsKey("first_name"));
            assertEquals("PENELOPE", currRecord.get("first_name"));
            assertTrue(currRecord.containsKey("last_name"));
            assertEquals("GUINESS", currRecord.get("last_name"));
            assertTrue(currRecord.containsKey("last_update"));
            assertEquals(df.parse("2006-02-15 04:34:33-00").toInstant(), ((Date) currRecord.get("last_update")).toInstant());

            currNodeContent = it.next();
            assertNotNull(currNodeContent.getData());
            assertNotNull(currNodeContent.getData().getRecord());
            currRecord = currNodeContent.getData().getRecord();
            assertTrue(currRecord.containsKey("actor_id"));
            assertEquals(2, currRecord.get("actor_id"));
            assertTrue(currRecord.containsKey("first_name"));
            assertEquals("NICK", currRecord.get("first_name"));
            assertTrue(currRecord.containsKey("last_name"));
            assertEquals("WAHLBERG", currRecord.get("last_name"));
            assertTrue(currRecord.containsKey("last_update"));
            assertEquals(df.parse("2006-02-15 04:34:33-00").toInstant(), ((Date) currRecord.get("last_update")).toInstant());

            currNodeContent = it.next();
            assertNotNull(currNodeContent.getData());
            assertNotNull(currNodeContent.getData().getRecord());
            currRecord = currNodeContent.getData().getRecord();
            assertTrue(currRecord.containsKey("actor_id"));
            assertEquals(3, currRecord.get("actor_id"));
            assertTrue(currRecord.containsKey("first_name"));
            assertEquals("ED", currRecord.get("first_name"));
            assertTrue(currRecord.containsKey("last_name"));
            assertEquals("CHASE", currRecord.get("last_name"));
            assertTrue(currRecord.containsKey("last_update"));
            assertEquals(df.parse("2006-02-15 04:34:33-00").toInstant(), ((Date) currRecord.get("last_update")).toInstant());

            currNodeContent = it.next();
            assertNotNull(currNodeContent.getData());
            assertNotNull(currNodeContent.getData().getRecord());
            currRecord = currNodeContent.getData().getRecord();
            assertTrue(currRecord.containsKey("actor_id"));
            assertEquals(4, currRecord.get("actor_id"));
            assertTrue(currRecord.containsKey("first_name"));
            assertEquals("JENNIFER", currRecord.get("first_name"));
            assertTrue(currRecord.containsKey("last_name"));
            assertEquals("DAVIS", currRecord.get("last_name"));
            assertTrue(currRecord.containsKey("last_update"));
            assertEquals(df.parse("2006-02-15 04:34:33-00").toInstant(), ((Date) currRecord.get("last_update")).toInstant());

            currNodeContent = it.next();
            assertNotNull(currNodeContent.getData());
            assertNotNull(currNodeContent.getData().getRecord());
            currRecord = currNodeContent.getData().getRecord();
            assertTrue(currRecord.containsKey("actor_id"));
            assertEquals(5, currRecord.get("actor_id"));
            assertTrue(currRecord.containsKey("first_name"));
            assertEquals("JOHNNY", currRecord.get("first_name"));
            assertTrue(currRecord.containsKey("last_name"));
            assertEquals("LOLLOBRIGIDA", currRecord.get("last_name"));
            assertTrue(currRecord.containsKey("last_update"));
            assertEquals(df.parse("2006-02-15 04:34:33-00").toInstant(), ((Date) currRecord.get("last_update")).toInstant());

            currNodeContent = it.next();
            assertNotNull(currNodeContent.getData());
            assertNotNull(currNodeContent.getData().getRecord());
            currRecord = currNodeContent.getData().getRecord();
            assertTrue(currRecord.containsKey("store_id"));
            assertEquals(1, currRecord.get("store_id"));
            assertTrue(currRecord.containsKey("manager_staff_id"));
            assertEquals(1, currRecord.get("manager_staff_id"));
            assertTrue(currRecord.containsKey("address_id"));
            assertEquals(1, currRecord.get("address_id"));
            assertTrue(currRecord.containsKey("last_update"));
            assertEquals(df.parse("2006-02-15 04:57:12-00").toInstant(), ((Date) currRecord.get("last_update")).toInstant());

            currNodeContent = it.next();
            assertNotNull(currNodeContent.getData());
            assertNotNull(currNodeContent.getData().getRecord());
            currRecord = currNodeContent.getData().getRecord();
            assertTrue(currRecord.containsKey("store_id"));
            assertEquals(2, currRecord.get("store_id"));
            assertTrue(currRecord.containsKey("manager_staff_id"));
            assertEquals(2, currRecord.get("manager_staff_id"));
            assertTrue(currRecord.containsKey("address_id"));
            assertEquals(2, currRecord.get("address_id"));
            assertTrue(currRecord.containsKey("last_update"));
            assertEquals(df.parse("2006-02-15 04:57:12-00").toInstant(), ((Date) currRecord.get("last_update")).toInstant());

        } catch (ParseException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Override
    @Test
    public void loadVerticesFromIdsSingleTableTest() throws Exception {

        /*
         * Fetching the first 5 vertices from the actor table by cyto-ids
         * cyto-id from is x_y where:
         *  - x is refers to the source table (actor is the first table in the source db schema)
         *  - y is the external key, then the pk value of the source record
         */

        String[] actorIds = {"1_1", "1_2", "1_3", "1_4", "1_5"};

        GraphData data = provider.load(dataSource, actorIds);

        assertEquals(data.getNodesClasses().size(), 1);
        assertEquals(data.getNodes().size(), 5);
        assertEquals(data.getEdgesClasses().size(), 0);
        assertEquals(data.getEdges().size(), 0);

        // Node classes checks
        assertTrue(data.getNodesClasses().containsKey("actor"));

        Map<String, Object> actorClass = data.getNodesClasses().get("actor");
        assertTrue(actorClass.containsKey("actor_id"));
        assertTrue(actorClass.containsKey("first_name"));
        assertTrue(actorClass.containsKey("last_name"));
        assertTrue(actorClass.containsKey("last_update"));

        // nodes checks
        Iterator<CytoData> it = data.getNodes().iterator();
        CytoData currNodeContent;
        Map<String, Object> currRecord;

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("actor_id"));
        assertEquals(currRecord.get("actor_id"), 1);
        assertTrue(currRecord.containsKey("first_name"));
        assertEquals(currRecord.get("first_name"), "PENELOPE");
        assertTrue(currRecord.containsKey("last_name"));
        assertEquals(currRecord.get("last_name"), "GUINESS");
        assertTrue(currRecord.containsKey("last_update"));

        assertEquals(df.parse("2006-02-15 04:34:33-00").toInstant(), ((Date) currRecord.get("last_update")).toInstant());

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("actor_id"));
        assertEquals(currRecord.get("actor_id"), 2);
        assertTrue(currRecord.containsKey("first_name"));
        assertEquals(currRecord.get("first_name"), "NICK");
        assertTrue(currRecord.containsKey("last_name"));
        assertEquals(currRecord.get("last_name"), "WAHLBERG");
        assertTrue(currRecord.containsKey("last_update"));
        assertEquals(df.parse("2006-02-15 04:34:33-00").toInstant(), ((Date) currRecord.get("last_update")).toInstant());

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("actor_id"));
        assertEquals(currRecord.get("actor_id"), 3);
        assertTrue(currRecord.containsKey("first_name"));
        assertEquals(currRecord.get("first_name"), "ED");
        assertTrue(currRecord.containsKey("last_name"));
        assertEquals(currRecord.get("last_name"), "CHASE");
        assertTrue(currRecord.containsKey("last_update"));
        assertEquals(df.parse("2006-02-15 04:34:33-00").toInstant(), ((Date) currRecord.get("last_update")).toInstant());

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("actor_id"));
        assertEquals(currRecord.get("actor_id"), 4);
        assertTrue(currRecord.containsKey("first_name"));
        assertEquals(currRecord.get("first_name"), "JENNIFER");
        assertTrue(currRecord.containsKey("last_name"));
        assertEquals(currRecord.get("last_name"), "DAVIS");
        assertTrue(currRecord.containsKey("last_update"));
        assertEquals(df.parse("2006-02-15 04:34:33-00").toInstant(), ((Date) currRecord.get("last_update")).toInstant());

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("actor_id"));
        assertEquals(currRecord.get("actor_id"), 5);
        assertTrue(currRecord.containsKey("first_name"));
        assertEquals(currRecord.get("first_name"), "JOHNNY");
        assertTrue(currRecord.containsKey("last_name"));
        assertEquals(currRecord.get("last_name"), "LOLLOBRIGIDA");
        assertTrue(currRecord.containsKey("last_update"));
        assertEquals(df.parse("2006-02-15 04:34:33-00").toInstant(), ((Date) currRecord.get("last_update")).toInstant());

        /*
         * Fetching 2 vertices from the store table by cyto-ids
         * cyto-id from is x_y where:
         *  - x is refers to the source table (actor is the first table in the source db schema)
         *  - y is the external key, then the pk value of the source record
         */

        String[] storeIds = {"16_1", "16_2"};
        data = provider.load(dataSource, storeIds);

        assertEquals(data.getNodesClasses().size(), 1);
        assertEquals(data.getNodes().size(), 2);
        assertEquals(data.getEdgesClasses().size(), 0);
        assertEquals(data.getEdges().size(), 0);

        // Node classes checks
        assertTrue(data.getNodesClasses().containsKey("store"));

        Map<String, Object> storeClass = data.getNodesClasses().get("store");
        assertTrue(storeClass.containsKey("store_id"));
        assertTrue(storeClass.containsKey("manager_staff_id"));
        assertTrue(storeClass.containsKey("address_id"));
        assertTrue(storeClass.containsKey("last_update"));

        // nodes checks
        it = data.getNodes().iterator();


        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("store_id"));
        assertEquals(currRecord.get("store_id"), 1);
        assertTrue(currRecord.containsKey("manager_staff_id"));
        assertEquals(currRecord.get("manager_staff_id"), 1);
        assertTrue(currRecord.containsKey("address_id"));
        assertEquals(currRecord.get("address_id"), 1);
        assertTrue(currRecord.containsKey("last_update"));
        assertEquals(((Date) currRecord.get("last_update")).toInstant(), df.parse("2006-02-15 04:57:12-00").toInstant());

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("store_id"));
        assertEquals(currRecord.get("store_id"), 2);
        assertTrue(currRecord.containsKey("manager_staff_id"));
        assertEquals(currRecord.get("manager_staff_id"), 2);
        assertTrue(currRecord.containsKey("address_id"));
        assertEquals(currRecord.get("address_id"), 2);
        assertTrue(currRecord.containsKey("last_update"));
        assertEquals(((Date) currRecord.get("last_update")).toInstant(), df.parse("2006-02-15 04:57:12-00").toInstant());

    }

    @Override
    @Test
    public void expand1To1RelWithSimpleFKTest() throws Exception {

        // 1-1 Relationship: distribution_country -[has_distribution_language]-> language

        // adding schema info
        Connection connection = null;
        Statement st = null;

        try {

            connection = DriverManager.getConnection(MySQLContainerHolder.container.getJdbcUrl(), MySQLContainerHolder.container.getUsername(), MySQLContainerHolder.container.getPassword());

            String distributioCountryTableBuilding = "create table distribution_country (distribution_country_id integer not null,"
                    + " name varchar(256) not null, distribution_language tinyint unsigned, primary key (distribution_country_id),"
                    + " foreign key (distribution_language) references language(language_id)) ENGINE=INNODB";
            st = connection.createStatement();
            st.execute(distributioCountryTableBuilding);

            // populating the new table
            String distributionCountryFilling =
                    "insert into distribution_country (distribution_country_id,name,distribution_language) values "
                            + "(1,'USA',1),"
                            + "(2,'Italy',2),"
                            + "(3,'Japan',3),"
                            + "(4,'China',4),"
                            + "(5,'France',5),"
                            + "(6,'Germany',6)";
            st.execute(distributionCountryFilling);

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
                fail();
            }
        }

        String[] rootIds = {"7_1", "7_2", "7_3", "7_4", "7_5", "7_6"};
        GraphData data = provider.expand(dataSource, rootIds, "out", "has_distribution_language", 300);

        assertEquals(data.getNodesClasses().size(), 1);
        assertEquals(data.getNodes().size(), 6);
        assertEquals(data.getEdgesClasses().size(), 1);
        assertEquals(data.getEdges().size(), 6);

        // Node classes checks
        assertTrue(data.getNodesClasses().containsKey("language"));

        Map<String, Object> languageClass = data.getNodesClasses().get("language");
        assertTrue(languageClass.containsKey("language_id"));
        assertTrue(languageClass.containsKey("name"));
        assertTrue(languageClass.containsKey("last_update"));

        // nodes checks
        Iterator<CytoData> it = data.getNodes().iterator();
        CytoData currNodeContent;
        Map<String, Object> currRecord;


        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("language_id"));
        assertEquals(currRecord.get("language_id"), 1);
        assertTrue(currRecord.containsKey("name"));
        assertEquals(((String) currRecord.get("name")).trim(), "English");
        assertTrue(currRecord.containsKey("last_update"));
        assertEquals(((Date) currRecord.get("last_update")).toInstant(), df.parse("2006-02-15 05:02:19-00").toInstant());


        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("language_id"));
        assertEquals(currRecord.get("language_id"), 2);
        assertTrue(currRecord.containsKey("name"));
        assertEquals(((String) currRecord.get("name")).trim(), "Italian");
        assertTrue(currRecord.containsKey("last_update"));
        assertEquals(((Date) currRecord.get("last_update")).toInstant(), df.parse("2006-02-15 05:02:19-00").toInstant());

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("language_id"));
        assertEquals(currRecord.get("language_id"), 3);
        assertTrue(currRecord.containsKey("name"));
        assertEquals(((String) currRecord.get("name")).trim(), "Japanese");
        assertTrue(currRecord.containsKey("last_update"));
        assertEquals(((Date) currRecord.get("last_update")).toInstant(), df.parse("2006-02-15 05:02:19-00").toInstant());

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("language_id"));
        assertEquals(currRecord.get("language_id"), 4);
        assertTrue(currRecord.containsKey("name"));
        assertEquals(((String) currRecord.get("name")).trim(), "Mandarin");
        assertTrue(currRecord.containsKey("last_update"));
        assertEquals(((Date) currRecord.get("last_update")).toInstant(), df.parse("2006-02-15 05:02:19-00").toInstant());

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("language_id"));
        assertEquals(currRecord.get("language_id"), 5);
        assertTrue(currRecord.containsKey("name"));
        assertEquals(((String) currRecord.get("name")).trim(), "French");
        assertTrue(currRecord.containsKey("last_update"));
        assertEquals(((Date) currRecord.get("last_update")).toInstant(), df.parse("2006-02-15 05:02:19-00").toInstant());

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("language_id"));
        assertEquals(currRecord.get("language_id"), 6);
        assertTrue(currRecord.containsKey("name"));
        assertEquals(((String) currRecord.get("name")).trim(), "German");
        assertTrue(currRecord.containsKey("last_update"));
        assertEquals(((Date) currRecord.get("last_update")).toInstant(), df.parse("2006-02-15 05:02:19-00").toInstant());


        // edges checks
        it = data.getEdges().iterator();
        CytoData currEdgeContent;

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_distribution_language");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "7_1");
        assertEquals(currEdgeContent.getData().getTarget(), "13_1");
        assertEquals(currEdgeContent.getData().getId(), "71_131");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_distribution_language");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "7_2");
        assertEquals(currEdgeContent.getData().getTarget(), "13_2");
        assertEquals(currEdgeContent.getData().getId(), "72_132");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_distribution_language");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "7_3");
        assertEquals(currEdgeContent.getData().getTarget(), "13_3");
        assertEquals(currEdgeContent.getData().getId(), "73_133");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_distribution_language");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "7_4");
        assertEquals(currEdgeContent.getData().getTarget(), "13_4");
        assertEquals(currEdgeContent.getData().getId(), "74_134");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_distribution_language");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "7_5");
        assertEquals(currEdgeContent.getData().getTarget(), "13_5");
        assertEquals(currEdgeContent.getData().getId(), "75_135");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_distribution_language");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "7_6");
        assertEquals(currEdgeContent.getData().getTarget(), "13_6");
        assertEquals(currEdgeContent.getData().getId(), "76_136");

        // 1-1 Relationship: language <-[has_distribution_language]- distribution_country

        rootIds[0] = "13_1";
        rootIds[1] = "13_2";
        rootIds[2] = "13_3";
        rootIds[3] = "13_4";
        rootIds[4] = "13_5";
        rootIds[5] = "13_6";
        data = provider.expand(dataSource, rootIds, "in", "has_distribution_language", 300);

        assertEquals(data.getNodesClasses().size(), 1);
        assertEquals(data.getNodes().size(), 6);
        assertEquals(data.getEdgesClasses().size(), 1);
        assertEquals(data.getEdges().size(), 6);

        // Node classes checks
        assertTrue(data.getNodesClasses().containsKey("distribution_country"));

        Map<String, Object> distributionCountryClass = data.getNodesClasses().get("distribution_country");
        assertTrue(distributionCountryClass.containsKey("distribution_country_id"));
        assertTrue(distributionCountryClass.containsKey("name"));
        assertTrue(distributionCountryClass.containsKey("distribution_language"));


        it = data.getNodes().iterator();

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("distribution_country_id"));
        assertEquals(currRecord.get("distribution_country_id"), 1);
        assertTrue(currRecord.containsKey("name"));
        assertEquals(((String) currRecord.get("name")), "USA");
        assertTrue(currRecord.containsKey("distribution_language"));
        assertEquals(currRecord.get("distribution_language"), 1);

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("distribution_country_id"));
        assertEquals(currRecord.get("distribution_country_id"), 2);
        assertTrue(currRecord.containsKey("name"));
        assertEquals(((String) currRecord.get("name")), "Italy");
        assertTrue(currRecord.containsKey("distribution_language"));
        assertEquals(currRecord.get("distribution_language"), 2);

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("distribution_country_id"));
        assertEquals(currRecord.get("distribution_country_id"), 3);
        assertTrue(currRecord.containsKey("name"));
        assertEquals(((String) currRecord.get("name")), "Japan");
        assertTrue(currRecord.containsKey("distribution_language"));
        assertEquals(currRecord.get("distribution_language"), 3);

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("distribution_country_id"));
        assertEquals(currRecord.get("distribution_country_id"), 4);
        assertTrue(currRecord.containsKey("name"));
        assertEquals(((String) currRecord.get("name")), "China");
        assertTrue(currRecord.containsKey("distribution_language"));
        assertEquals(currRecord.get("distribution_language"), 4);

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("distribution_country_id"));
        assertEquals(currRecord.get("distribution_country_id"), 5);
        assertTrue(currRecord.containsKey("name"));
        assertEquals(((String) currRecord.get("name")), "France");
        assertTrue(currRecord.containsKey("distribution_language"));
        assertEquals(currRecord.get("distribution_language"), 5);

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("distribution_country_id"));
        assertEquals(currRecord.get("distribution_country_id"), 6);
        assertTrue(currRecord.containsKey("name"));
        assertEquals(((String) currRecord.get("name")), "Germany");
        assertTrue(currRecord.containsKey("distribution_language"));
        assertEquals(currRecord.get("distribution_language"), 6);

        // edges checks
        it = data.getEdges().iterator();

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_distribution_language");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "7_1");
        assertEquals(currEdgeContent.getData().getTarget(), "13_1");
        assertEquals(currEdgeContent.getData().getId(), "71_131");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_distribution_language");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "7_2");
        assertEquals(currEdgeContent.getData().getTarget(), "13_2");
        assertEquals(currEdgeContent.getData().getId(), "72_132");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_distribution_language");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "7_3");
        assertEquals(currEdgeContent.getData().getTarget(), "13_3");
        assertEquals(currEdgeContent.getData().getId(), "73_133");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_distribution_language");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "7_4");
        assertEquals(currEdgeContent.getData().getTarget(), "13_4");
        assertEquals(currEdgeContent.getData().getId(), "74_134");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_distribution_language");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "7_5");
        assertEquals(currEdgeContent.getData().getTarget(), "13_5");
        assertEquals(currEdgeContent.getData().getId(), "75_135");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_distribution_language");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "7_6");
        assertEquals(currEdgeContent.getData().getTarget(), "13_6");
        assertEquals(currEdgeContent.getData().getId(), "76_136");

        // dropping the new added table
        try {

            connection = DriverManager.getConnection(MySQLContainerHolder.container.getJdbcUrl(), MySQLContainerHolder.container.getUsername(), MySQLContainerHolder.container.getPassword());

            String deleteDistributionCountryTable = "drop table distribution_country";
            st = connection.createStatement();
            st.execute(deleteDistributionCountryTable);

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
                fail();
            }
        }
    }

    @Override
    @Test
    public void expand1ToNRelWithSimpleFKTest() {

        // 1-N Relationship: city -[has_country]-> country

        String[] rootIds = {"4_1"};
        GraphData data = provider.expand(dataSource, rootIds, "out", "has_country", 300);

        assertEquals(data.getNodesClasses().size(), 1);
        assertEquals(data.getNodes().size(), 1);
        assertEquals(data.getEdgesClasses().size(), 1);
        assertEquals(data.getEdges().size(), 1);

        // Node classes checks
        assertTrue(data.getNodesClasses().containsKey("country"));

        Map<String, Object> customerClass = data.getNodesClasses().get("country");
        assertTrue(customerClass.containsKey("country_id"));
        assertTrue(customerClass.containsKey("country"));
        assertTrue(customerClass.containsKey("last_update"));

        // nodes checks
        Iterator<CytoData> it = data.getNodes().iterator();
        CytoData currNodeContent;
        Map<String, Object> currRecord;

        try {

            currNodeContent = it.next();
            assertNotNull(currNodeContent.getData());
            assertNotNull(currNodeContent.getData().getRecord());
            currRecord = currNodeContent.getData().getRecord();
            assertTrue(currRecord.containsKey("country_id"));
            assertEquals(currRecord.get("country_id"), 87);
            assertTrue(currRecord.containsKey("country"));
            assertEquals(currRecord.get("country"), "Spain");
            assertTrue(currRecord.containsKey("last_update"));
            assertEquals(((Date) currRecord.get("last_update")).toInstant(), df.parse("2006-02-15 04:44:00-00").toInstant());

        } catch (ParseException e) {
            e.printStackTrace();
            fail();
        }


        // edges checks
        it = data.getEdges().iterator();
        CytoData currEdgeContent;

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);

        assertEquals(currEdgeContent.getClasses(), "has_country");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "4_1");
        assertEquals(currEdgeContent.getData().getTarget(), "5_87");
        assertEquals(currEdgeContent.getData().getId(), "41_587");

        // 1-N Relationship: city <-[has_country]- country

        rootIds[0] = "5_87";
        data = provider.expand(dataSource, rootIds, "in", "has_country", 300);

        assertEquals(data.getNodesClasses().size(), 1);
        assertEquals(data.getNodes().size(), 5);
        assertEquals(data.getEdgesClasses().size(), 1);
        assertEquals(data.getEdges().size(), 5);

        // nodes checks
        it = data.getNodes().iterator();

        try {

            currNodeContent = it.next();
            assertNotNull(currNodeContent.getData());
            assertNotNull(currNodeContent.getData().getRecord());
            currRecord = currNodeContent.getData().getRecord();
            assertTrue(currRecord.containsKey("city_id"));
            assertEquals(currRecord.get("city_id"), 1);
            assertTrue(currRecord.containsKey("city"));
            assertEquals(currRecord.get("city"), "A Corua (La Corua)");
            assertTrue(currRecord.containsKey("country_id"));
            assertEquals(currRecord.get("country_id"), 87);
            assertTrue(currRecord.containsKey("last_update"));
            assertEquals(((Date) currRecord.get("last_update")).toInstant(), df.parse("2006-02-15 04:45:25-00").toInstant());

            currNodeContent = it.next();
            assertNotNull(currNodeContent.getData());
            assertNotNull(currNodeContent.getData().getRecord());
            currRecord = currNodeContent.getData().getRecord();
            assertTrue(currRecord.containsKey("city_id"));
            assertEquals(currRecord.get("city_id"), 146);
            assertTrue(currRecord.containsKey("city"));
            assertEquals(currRecord.get("city"), "Donostia-San Sebastin");
            assertTrue(currRecord.containsKey("country_id"));
            assertEquals(currRecord.get("country_id"), 87);
            assertTrue(currRecord.containsKey("last_update"));
            assertEquals(((Date) currRecord.get("last_update")).toInstant(), df.parse("2006-02-15 04:45:25-00").toInstant());

            currNodeContent = it.next();
            assertNotNull(currNodeContent.getData());
            assertNotNull(currNodeContent.getData().getRecord());
            currRecord = currNodeContent.getData().getRecord();
            assertTrue(currRecord.containsKey("city_id"));
            assertEquals(currRecord.get("city_id"), 181);
            assertTrue(currRecord.containsKey("city"));
            assertEquals(currRecord.get("city"), "Gijn");
            assertTrue(currRecord.containsKey("country_id"));
            assertEquals(currRecord.get("country_id"), 87);
            assertTrue(currRecord.containsKey("last_update"));
            assertEquals(((Date) currRecord.get("last_update")).toInstant(), df.parse("2006-02-15 04:45:25-00").toInstant());

            currNodeContent = it.next();
            assertNotNull(currNodeContent.getData());
            assertNotNull(currNodeContent.getData().getRecord());
            currRecord = currNodeContent.getData().getRecord();
            assertTrue(currRecord.containsKey("city_id"));
            assertEquals(currRecord.get("city_id"), 388);
            assertTrue(currRecord.containsKey("city"));
            assertEquals(currRecord.get("city"), "Ourense (Orense)");
            assertTrue(currRecord.containsKey("country_id"));
            assertEquals(currRecord.get("country_id"), 87);
            assertTrue(currRecord.containsKey("last_update"));
            assertEquals(((Date) currRecord.get("last_update")).toInstant(), df.parse("2006-02-15 04:45:25-00").toInstant());

            currNodeContent = it.next();
            assertNotNull(currNodeContent.getData());
            assertNotNull(currNodeContent.getData().getRecord());
            currRecord = currNodeContent.getData().getRecord();
            assertTrue(currRecord.containsKey("city_id"));
            assertEquals(currRecord.get("city_id"), 459);
            assertTrue(currRecord.containsKey("city"));
            assertEquals(currRecord.get("city"), "Santiago de Compostela");
            assertTrue(currRecord.containsKey("country_id"));
            assertEquals(currRecord.get("country_id"), 87);
            assertTrue(currRecord.containsKey("last_update"));
            assertEquals(((Date) currRecord.get("last_update")).toInstant(), df.parse("2006-02-15 04:45:25-00").toInstant());

        } catch (ParseException e) {
            e.printStackTrace();
            fail();
        }

        // edges checks
        it = data.getEdges().iterator();

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_country");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "4_1");
        assertEquals(currEdgeContent.getData().getTarget(), "5_87");
        assertEquals(currEdgeContent.getData().getId(), "41_587");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_country");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "4_146");
        assertEquals(currEdgeContent.getData().getTarget(), "5_87");
        assertEquals(currEdgeContent.getData().getId(), "4146_587");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_country");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "4_181");
        assertEquals(currEdgeContent.getData().getTarget(), "5_87");
        assertEquals(currEdgeContent.getData().getId(), "4181_587");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_country");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "4_388");
        assertEquals(currEdgeContent.getData().getTarget(), "5_87");
        assertEquals(currEdgeContent.getData().getId(), "4388_587");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_country");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "4_459");
        assertEquals(currEdgeContent.getData().getTarget(), "5_87");
        assertEquals(currEdgeContent.getData().getId(), "4459_587");

    }

    @Override
    @Test
    public void expandMultiple1ToNRelWithSimpleFKTest() {

        // 1-N Relationship: rental -[has_customer]-> customer

        String[] rootIds = {"14_1"};
        GraphData data = provider.expand(dataSource, rootIds, "out", "has_customer", 300);

        assertEquals(data.getNodesClasses().size(), 1);
        assertEquals(data.getNodes().size(), 1);
        assertEquals(data.getEdgesClasses().size(), 1);
        assertEquals(data.getEdges().size(), 1);

        // Node classes checks
        assertTrue(data.getNodesClasses().containsKey("customer"));

        Map<String, Object> customerClass = data.getNodesClasses().get("customer");
        assertTrue(customerClass.containsKey("customer_id"));
        assertTrue(customerClass.containsKey("store_id"));
        assertTrue(customerClass.containsKey("first_name"));
        assertTrue(customerClass.containsKey("last_name"));
        assertTrue(customerClass.containsKey("email"));
        assertTrue(customerClass.containsKey("address_id"));
        assertTrue(customerClass.containsKey("create_date"));
        assertTrue(customerClass.containsKey("last_update"));
        assertTrue(customerClass.containsKey("active"));

        // nodes checks
        Iterator<CytoData> it = data.getNodes().iterator();
        CytoData currNodeContent;
        Map<String, Object> currRecord;

        try {

            currNodeContent = it.next();
            assertNotNull(currNodeContent.getData());
            assertNotNull(currNodeContent.getData().getRecord());
            currRecord = currNodeContent.getData().getRecord();
            assertTrue(currRecord.containsKey("customer_id"));
            assertEquals(currRecord.get("customer_id"), 130);
            assertTrue(currRecord.containsKey("store_id"));
            assertEquals(currRecord.get("store_id"), 1);
            assertTrue(currRecord.containsKey("first_name"));
            assertEquals(currRecord.get("first_name"), "CHARLOTTE");
            assertTrue(currRecord.containsKey("last_name"));
            assertEquals(currRecord.get("last_name"), "HUNTER");
            assertTrue(currRecord.containsKey("email"));
            assertEquals(currRecord.get("email"), "CHARLOTTE.HUNTER@sakilacustomer.org");
            assertTrue(currRecord.containsKey("address_id"));
            assertEquals(currRecord.get("address_id"), 134);
            assertTrue(currRecord.containsKey("create_date"));
            assertEquals(((Date) currRecord.get("create_date")).toInstant(), df.parse("2006-02-14 22:04:36-00").toInstant());
            assertTrue(currRecord.containsKey("last_update"));
            assertEquals(((Date) currRecord.get("last_update")).toInstant(), df.parse("2006-02-15 04:57:20-00").toInstant());
            assertTrue(currRecord.containsKey("active"));
            assertEquals(currRecord.get("active"), true);

        } catch (ParseException e) {
            e.printStackTrace();
            fail();
        }

        // edges checks
        it = data.getEdges().iterator();
        CytoData currEdgeContent;

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);

        assertEquals(currEdgeContent.getClasses(), "has_customer");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "14_1");
        assertEquals(currEdgeContent.getData().getTarget(), "6_130");
        assertEquals(currEdgeContent.getData().getId(), "141_6130");

        // 1-N Relationship: customer <-[has_customer]- rental

        rootIds[0] = "6_130";
        data = provider.expand(dataSource, rootIds, "in", "has_customer", 300);

        assertEquals(data.getNodesClasses().size(), 2);
        assertEquals(data.getNodes().size(), 48);
        assertEquals(data.getEdgesClasses().size(), 1);
        assertEquals(data.getEdges().size(), 48);

        // Node classes checks
        assertTrue(data.getNodesClasses().containsKey("rental"));
        assertTrue(data.getNodesClasses().containsKey("payment"));  // has_customer represents also the relationship: Payment -> Customer

        // nodes checks
        Set<CytoData> rentalNodes = data.getNodes().stream()
                .filter(rentalNode -> rentalNode.getClasses().equals("rental"))
                .collect(Collectors.toSet());
        Set<CytoData> paymentNodes = data.getNodes().stream()
                .filter(rentalNode -> rentalNode.getClasses().equals("payment"))
                .collect(Collectors.toSet());

        assertEquals(rentalNodes.size(), 24);
        assertEquals(paymentNodes.size(), 24);
    }

    @Override
    @Test
    public void expand1ToNRelWithJoinTableAndSimpleFKTest() {

        /**
         * Get movies by actor
         */

        // expanding 1-N relationship: actor <-[has_actor]- film_actor
        String[] rootIds = {"1_1"};
        GraphData data = provider.expand(dataSource, rootIds, "in", "has_actor", 300);

        assertEquals(data.getNodesClasses().size(), 1);
        assertEquals(data.getNodes().size(), 19);
        assertEquals(data.getEdgesClasses().size(), 1);
        assertEquals(data.getEdges().size(), 19);

        // Node classes checks
        assertTrue(data.getNodesClasses().containsKey("film_actor"));

        Map<String, Object> filmActorClass = data.getNodesClasses().get("film_actor");
        assertTrue(filmActorClass.containsKey("film_id"));
        assertTrue(filmActorClass.containsKey("actor_id"));
        assertTrue(filmActorClass.containsKey("last_update"));


        String[] joinRecordRoots = new String[19];
        int i = 0;
        for (CytoData currentData : data.getNodes()) {
            joinRecordRoots[i] = "8_1_" + currentData.getData().getRecord().get("film_id").toString();
            i++;
        }

        // expanding 1-N relationship: film_actor -[has_film] -> film

        data = provider.expand(dataSource, joinRecordRoots, "out", "has_film", 300);

        assertEquals(data.getNodesClasses().size(), 1);
        assertEquals(data.getNodes().size(), 19);
        assertEquals(data.getEdgesClasses().size(), 1);
        assertEquals(data.getEdges().size(), 19);

        // Node classes checks
        assertTrue(data.getNodesClasses().containsKey("film"));

        Map<String, Object> filmClass = data.getNodesClasses().get("film");
        assertTrue(filmClass.containsKey("film_id"));
        assertTrue(filmClass.containsKey("title"));
        assertTrue(filmClass.containsKey("description"));
        assertTrue(filmClass.containsKey("release_year"));
        assertTrue(filmClass.containsKey("language_id"));
        assertTrue(filmClass.containsKey("original_language_id"));
        assertTrue(filmClass.containsKey("rental_duration"));
        assertTrue(filmClass.containsKey("rental_rate"));
        assertTrue(filmClass.containsKey("length"));
        assertTrue(filmClass.containsKey("replacement_cost"));
        assertTrue(filmClass.containsKey("rating"));
        assertTrue(filmClass.containsKey("last_update"));
        assertTrue(filmClass.containsKey("special_features"));

        // nodes checks
        Iterator<CytoData> it = data.getNodes().iterator();
        CytoData currNodeContent;
        Map<String, Object> currRecord;

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("film_id"));
        assertEquals(currRecord.get("film_id"), 1);
        assertTrue(currRecord.containsKey("title"));
        assertEquals(currRecord.get("title"), "ACADEMY DINOSAUR");
        assertTrue(currRecord.containsKey("description"));
        assertNotNull(currRecord.get("description"));

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("film_id"));
        assertEquals(currRecord.get("film_id"), 23);
        assertTrue(currRecord.containsKey("title"));
        assertEquals(currRecord.get("title"), "ANACONDA CONFESSIONS");
        assertTrue(currRecord.containsKey("description"));
        assertNotNull(currRecord.get("description"));

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("film_id"));
        assertEquals(currRecord.get("film_id"), 25);
        assertTrue(currRecord.containsKey("title"));
        assertEquals(currRecord.get("title"), "ANGELS LIFE");
        assertTrue(currRecord.containsKey("description"));
        assertNotNull(currRecord.get("description"));

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("film_id"));
        assertEquals(currRecord.get("film_id"), 106);
        assertTrue(currRecord.containsKey("title"));
        assertEquals(currRecord.get("title"), "BULWORTH COMMANDMENTS");
        assertTrue(currRecord.containsKey("description"));
        assertNotNull(currRecord.get("description"));

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("film_id"));
        assertEquals(currRecord.get("film_id"), 140);
        assertTrue(currRecord.containsKey("title"));
        assertEquals(currRecord.get("title"), "CHEAPER CLYDE");
        assertTrue(currRecord.containsKey("description"));
        assertNotNull(currRecord.get("description"));

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("film_id"));
        assertEquals(currRecord.get("film_id"), 166);
        assertTrue(currRecord.containsKey("title"));
        assertEquals(currRecord.get("title"), "COLOR PHILADELPHIA");
        assertTrue(currRecord.containsKey("description"));
        assertNotNull(currRecord.get("description"));

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("film_id"));
        assertEquals(currRecord.get("film_id"), 277);
        assertTrue(currRecord.containsKey("title"));
        assertEquals(currRecord.get("title"), "ELEPHANT TROJAN");
        assertTrue(currRecord.containsKey("description"));
        assertNotNull(currRecord.get("description"));

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("film_id"));
        assertEquals(currRecord.get("film_id"), 361);
        assertTrue(currRecord.containsKey("title"));
        assertEquals(currRecord.get("title"), "GLEAMING JAWBREAKER");
        assertTrue(currRecord.containsKey("description"));
        assertNotNull(currRecord.get("description"));

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("film_id"));
        assertEquals(currRecord.get("film_id"), 438);
        assertTrue(currRecord.containsKey("title"));
        assertEquals(currRecord.get("title"), "HUMAN GRAFFITI");
        assertTrue(currRecord.containsKey("description"));
        assertNotNull(currRecord.get("description"));

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("film_id"));
        assertEquals(currRecord.get("film_id"), 499);
        assertTrue(currRecord.containsKey("title"));
        assertEquals(currRecord.get("title"), "KING EVOLUTION");
        assertTrue(currRecord.containsKey("description"));
        assertNotNull(currRecord.get("description"));

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("film_id"));
        assertEquals(currRecord.get("film_id"), 506);
        assertTrue(currRecord.containsKey("title"));
        assertEquals(currRecord.get("title"), "LADY STAGE");
        assertTrue(currRecord.containsKey("description"));
        assertNotNull(currRecord.get("description"));

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("film_id"));
        assertEquals(currRecord.get("film_id"), 509);
        assertTrue(currRecord.containsKey("title"));
        assertEquals(currRecord.get("title"), "LANGUAGE COWBOY");
        assertTrue(currRecord.containsKey("description"));
        assertNotNull(currRecord.get("description"));

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("film_id"));
        assertEquals(currRecord.get("film_id"), 605);
        assertTrue(currRecord.containsKey("title"));
        assertEquals(currRecord.get("title"), "MULHOLLAND BEAST");
        assertTrue(currRecord.containsKey("description"));
        assertNotNull(currRecord.get("description"));

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("film_id"));
        assertEquals(currRecord.get("film_id"), 635);
        assertTrue(currRecord.containsKey("title"));
        assertEquals(currRecord.get("title"), "OKLAHOMA JUMANJI");
        assertTrue(currRecord.containsKey("description"));
        assertNotNull(currRecord.get("description"));

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("film_id"));
        assertEquals(currRecord.get("film_id"), 749);
        assertTrue(currRecord.containsKey("title"));
        assertEquals(currRecord.get("title"), "RULES HUMAN");
        assertTrue(currRecord.containsKey("description"));
        assertNotNull(currRecord.get("description"));

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("film_id"));
        assertEquals(currRecord.get("film_id"), 832);
        assertTrue(currRecord.containsKey("title"));
        assertEquals(currRecord.get("title"), "SPLASH GUMP");
        assertTrue(currRecord.containsKey("description"));
        assertNotNull(currRecord.get("description"));

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("film_id"));
        assertEquals(currRecord.get("film_id"), 939);
        assertTrue(currRecord.containsKey("title"));
        assertEquals(currRecord.get("title"), "VERTIGO NORTHWEST");
        assertTrue(currRecord.containsKey("description"));
        assertNotNull(currRecord.get("description"));

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("film_id"));
        assertEquals(currRecord.get("film_id"), 970);
        assertTrue(currRecord.containsKey("title"));
        assertEquals(currRecord.get("title"), "WESTWARD SEABISCUIT");
        assertTrue(currRecord.containsKey("description"));
        assertNotNull(currRecord.get("description"));

        currNodeContent = it.next();
        assertNotNull(currNodeContent.getData());
        assertNotNull(currNodeContent.getData().getRecord());
        currRecord = currNodeContent.getData().getRecord();
        assertTrue(currRecord.containsKey("film_id"));
        assertEquals(currRecord.get("film_id"), 980);
        assertTrue(currRecord.containsKey("title"));
        assertEquals(currRecord.get("title"), "WIZARD COLDBLOODED");
        assertTrue(currRecord.containsKey("description"));
        assertNotNull(currRecord.get("description"));

        // edges checks
        it = data.getEdges().iterator();
        CytoData currEdgeContent;

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_film");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "8_1_1");
        assertEquals(currEdgeContent.getData().getTarget(), "7_1");
        assertEquals(currEdgeContent.getData().getId(), "811_71");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_film");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "8_1_23");
        assertEquals(currEdgeContent.getData().getTarget(), "7_23");
        assertEquals(currEdgeContent.getData().getId(), "8123_723");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_film");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "8_1_25");
        assertEquals(currEdgeContent.getData().getTarget(), "7_25");
        assertEquals(currEdgeContent.getData().getId(), "8125_725");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_film");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "8_1_106");
        assertEquals(currEdgeContent.getData().getTarget(), "7_106");
        assertEquals(currEdgeContent.getData().getId(), "81106_7106");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_film");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "8_1_140");
        assertEquals(currEdgeContent.getData().getTarget(), "7_140");
        assertEquals(currEdgeContent.getData().getId(), "81140_7140");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_film");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "8_1_166");
        assertEquals(currEdgeContent.getData().getTarget(), "7_166");
        assertEquals(currEdgeContent.getData().getId(), "81166_7166");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_film");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "8_1_277");
        assertEquals(currEdgeContent.getData().getTarget(), "7_277");
        assertEquals(currEdgeContent.getData().getId(), "81277_7277");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_film");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "8_1_361");
        assertEquals(currEdgeContent.getData().getTarget(), "7_361");
        assertEquals(currEdgeContent.getData().getId(), "81361_7361");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_film");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "8_1_438");
        assertEquals(currEdgeContent.getData().getTarget(), "7_438");
        assertEquals(currEdgeContent.getData().getId(), "81438_7438");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_film");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "8_1_499");
        assertEquals(currEdgeContent.getData().getTarget(), "7_499");
        assertEquals(currEdgeContent.getData().getId(), "81499_7499");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_film");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "8_1_506");
        assertEquals(currEdgeContent.getData().getTarget(), "7_506");
        assertEquals(currEdgeContent.getData().getId(), "81506_7506");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_film");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "8_1_509");
        assertEquals(currEdgeContent.getData().getTarget(), "7_509");
        assertEquals(currEdgeContent.getData().getId(), "81509_7509");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_film");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "8_1_605");
        assertEquals(currEdgeContent.getData().getTarget(), "7_605");
        assertEquals(currEdgeContent.getData().getId(), "81605_7605");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_film");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "8_1_635");
        assertEquals(currEdgeContent.getData().getTarget(), "7_635");
        assertEquals(currEdgeContent.getData().getId(), "81635_7635");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_film");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "8_1_749");
        assertEquals(currEdgeContent.getData().getTarget(), "7_749");
        assertEquals(currEdgeContent.getData().getId(), "81749_7749");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_film");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "8_1_832");
        assertEquals(currEdgeContent.getData().getTarget(), "7_832");
        assertEquals(currEdgeContent.getData().getId(), "81832_7832");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_film");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "8_1_939");
        assertEquals(currEdgeContent.getData().getTarget(), "7_939");
        assertEquals(currEdgeContent.getData().getId(), "81939_7939");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_film");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "8_1_970");
        assertEquals(currEdgeContent.getData().getTarget(), "7_970");
        assertEquals(currEdgeContent.getData().getId(), "81970_7970");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_film");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "8_1_980");
        assertEquals(currEdgeContent.getData().getTarget(), "7_980");
        assertEquals(currEdgeContent.getData().getId(), "81980_7980");


        /**
         * Get actors by movie
         */

        // expanding 1-N relationship: film <-[has_film]- film_actor

        rootIds[0] = "7_2";
        data = provider.expand(dataSource, rootIds, "in", "has_film", 300);

        assertEquals(data.getNodesClasses().size(), 3);
        assertEquals(data.getNodes().size(), 8);
        assertEquals(data.getEdgesClasses().size(), 1);
        assertEquals(data.getEdges().size(), 8);

        // Node classes checks
        assertTrue(data.getNodesClasses().containsKey("film_actor"));
        assertTrue(data.getNodesClasses().containsKey("film_category"));    // has_film represents also the relationship: film_category -> film
        assertTrue(data.getNodesClasses().containsKey("inventory"));    // has_film represents also the relationship: inventory -> film

        filmActorClass = data.getNodesClasses().get("film_actor");
        assertTrue(filmActorClass.containsKey("film_id"));
        assertTrue(filmActorClass.containsKey("actor_id"));
        assertTrue(filmActorClass.containsKey("last_update"));
        filmActorClass = data.getNodesClasses().get("film_category");
        assertTrue(filmActorClass.containsKey("film_id"));
        assertTrue(filmActorClass.containsKey("category_id"));
        assertTrue(filmActorClass.containsKey("last_update"));
        Map<String, Object> inventoryClass = data.getNodesClasses().get("inventory");
        assertTrue(inventoryClass.containsKey("inventory_id"));
        assertTrue(inventoryClass.containsKey("film_id"));
        assertTrue(inventoryClass.containsKey("store_id"));
        assertTrue(inventoryClass.containsKey("last_update"));

        Set<CytoData> filmActorNodes = data.getNodes().stream()
                .filter(filmActorNode -> filmActorNode.getClasses().equals("film_actor"))
                .collect(Collectors.toSet());
        Set<CytoData> filmCategoryNodes = data.getNodes().stream()
                .filter(filmCategoryNode -> filmCategoryNode.getClasses().equals("film_category"))
                .collect(Collectors.toSet());
        Set<CytoData> inventoryNodes = data.getNodes().stream()
                .filter(inventoryNode -> inventoryNode.getClasses().equals("inventory"))
                .collect(Collectors.toSet());

        assertEquals(filmActorNodes.size(), 4);
        assertEquals(filmCategoryNodes.size(), 1);
        assertEquals(inventoryNodes.size(), 3);

        joinRecordRoots = new String[4];
        i = 0;
        for (CytoData currentData : filmActorNodes) {
            joinRecordRoots[i] = "8_" + currentData.getData().getRecord().get("actor_id").toString() + "_2";
            i++;
        }

        // expanding 1-N relationship: film_actor -[has_actor]-> actor

        data = provider.expand(dataSource, joinRecordRoots, "out", "has_actor", 300);

        assertEquals(data.getNodesClasses().size(), 1);
        assertEquals(data.getNodes().size(), 4);
        assertEquals(data.getEdgesClasses().size(), 1);
        assertEquals(data.getEdges().size(), 4);

        // Node classes checks
        assertTrue(data.getNodesClasses().containsKey("actor"));

        Map<String, Object> actorClass = data.getNodesClasses().get("actor");
        assertTrue(actorClass.containsKey("actor_id"));
        assertTrue(actorClass.containsKey("first_name"));
        assertTrue(actorClass.containsKey("last_name"));
        assertTrue(actorClass.containsKey("last_update"));

        // nodes checks
        it = data.getNodes().iterator();

        try {

            currNodeContent = it.next();
            assertNotNull(currNodeContent.getData());
            assertNotNull(currNodeContent.getData().getRecord());
            currRecord = currNodeContent.getData().getRecord();
            assertTrue(currRecord.containsKey("actor_id"));
            assertEquals(currRecord.get("actor_id"), 19);
            assertTrue(currRecord.containsKey("first_name"));
            assertEquals(currRecord.get("first_name"), "BOB");
            assertTrue(currRecord.containsKey("last_name"));
            assertEquals(currRecord.get("last_name"), "FAWCETT");
            assertTrue(currRecord.containsKey("last_update"));
            assertEquals(((Date) currRecord.get("last_update")).toInstant(), df.parse("2006-02-15 04:34:33-00").toInstant());

            currNodeContent = it.next();
            assertNotNull(currNodeContent.getData());
            assertNotNull(currNodeContent.getData().getRecord());
            currRecord = currNodeContent.getData().getRecord();
            assertTrue(currRecord.containsKey("actor_id"));
            assertEquals(currRecord.get("actor_id"), 85);
            assertTrue(currRecord.containsKey("first_name"));
            assertEquals(currRecord.get("first_name"), "MINNIE");
            assertTrue(currRecord.containsKey("last_name"));
            assertEquals(currRecord.get("last_name"), "ZELLWEGER");
            assertTrue(currRecord.containsKey("last_update"));
            assertEquals(((Date) currRecord.get("last_update")).toInstant(), df.parse("2006-02-15 04:34:33-00").toInstant());

            currNodeContent = it.next();
            assertNotNull(currNodeContent.getData());
            assertNotNull(currNodeContent.getData().getRecord());
            currRecord = currNodeContent.getData().getRecord();
            assertTrue(currRecord.containsKey("actor_id"));
            assertEquals(currRecord.get("actor_id"), 90);
            assertTrue(currRecord.containsKey("first_name"));
            assertEquals(currRecord.get("first_name"), "SEAN");
            assertTrue(currRecord.containsKey("last_name"));
            assertEquals(currRecord.get("last_name"), "GUINESS");
            assertTrue(currRecord.containsKey("last_update"));
            assertEquals(((Date) currRecord.get("last_update")).toInstant(), df.parse("2006-02-15 04:34:33-00").toInstant());

            currNodeContent = it.next();
            assertNotNull(currNodeContent.getData());
            assertNotNull(currNodeContent.getData().getRecord());
            currRecord = currNodeContent.getData().getRecord();
            assertTrue(currRecord.containsKey("actor_id"));
            assertEquals(currRecord.get("actor_id"), 160);
            assertTrue(currRecord.containsKey("first_name"));
            assertEquals(currRecord.get("first_name"), "CHRIS");
            assertTrue(currRecord.containsKey("last_name"));
            assertEquals(currRecord.get("last_name"), "DEPP");
            assertTrue(currRecord.containsKey("last_update"));
            assertEquals(((Date) currRecord.get("last_update")).toInstant(), df.parse("2006-02-15 04:34:33-00").toInstant());

        } catch (ParseException e) {
            e.printStackTrace();
            fail();
        }

        // edges checks
        it = data.getEdges().iterator();

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_actor");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "8_19_2");
        assertEquals(currEdgeContent.getData().getTarget(), "1_19");
        assertEquals(currEdgeContent.getData().getId(), "8192_119");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_actor");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "8_85_2");
        assertEquals(currEdgeContent.getData().getTarget(), "1_85");
        assertEquals(currEdgeContent.getData().getId(), "8852_185");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_actor");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "8_90_2");
        assertEquals(currEdgeContent.getData().getTarget(), "1_90");
        assertEquals(currEdgeContent.getData().getId(), "8902_190");

        currEdgeContent = it.next();
        assertNotNull(currEdgeContent.getData());
        assertNotNull(currEdgeContent.getData().getRecord());
        currRecord = currEdgeContent.getData().getRecord();
        assertEquals(currRecord.size(), 0);
        assertEquals(currEdgeContent.getClasses(), "has_actor");
        assertEquals(currEdgeContent.getGroup(), "edges");
        assertEquals(currEdgeContent.getData().getSource(), "8_160_2");
        assertEquals(currEdgeContent.getData().getTarget(), "1_160");
        assertEquals(currEdgeContent.getData().getId(), "81602_1160");
    }

    @Override
    @Test
    public void expand1To1RelWithCompositeFKTest() {

    }

    @Override
    @Test
    public void expand1ToNRelWithCompositeFKTest() {

    }

    @Override
    @Test
    public void expand1ToNRelWithJoinTableAndCompositeFKTest() {

    }
}
