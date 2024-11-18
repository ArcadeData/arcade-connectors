package com.arcadeanalytics.provider.rdbms.dataprovider;

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.arcadeanalytics.provider.CytoData;
import com.arcadeanalytics.provider.DataSourceInfo;
import com.arcadeanalytics.provider.GraphData;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;

class MysqlSQLDataProviderTest extends AbstractRDBMSProviderTest {

    private static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssX");

    private DataSourceInfo dataSource = null;

    @BeforeEach
    public void setUp() throws Exception {
        final MySQLContainer container = MySQLContainerHolder.container;
        this.dataSource =
            new DataSourceInfo(
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

        assertThat(data.getNodesClasses().size()).isEqualTo(1);
        assertThat(data.getNodes().size()).isEqualTo(5);
        assertThat(data.getEdgesClasses().size()).isEqualTo(0);
        assertThat(data.getEdges().size()).isEqualTo(0);

        // Node classes checks
        assertThat(data.getNodesClasses().containsKey("actor")).isTrue();

        Map<String, Object> actorClass = data.getNodesClasses().get("actor");
        assertThat(actorClass.containsKey("actor_id")).isTrue();
        assertThat(actorClass.containsKey("first_name")).isTrue();
        assertThat(actorClass.containsKey("last_name")).isTrue();
        assertThat(actorClass.containsKey("last_update")).isTrue();
        // nodes checks
        Iterator<CytoData> it = data.getNodes().iterator();
        CytoData currNodeContent;
        Map<String, Object> currRecord;

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("actor_id")).isTrue();
        assertThat(currRecord.get("actor_id")).isEqualTo(1);
        assertThat(currRecord.containsKey("first_name")).isTrue();
        assertThat(currRecord.get("first_name")).isEqualTo("PENELOPE");
        assertThat(currRecord.containsKey("last_name")).isTrue();
        assertThat(currRecord.get("last_name")).isEqualTo("GUINESS");
        assertThat(currRecord.containsKey("last_update")).isTrue();
        //        assertEquals("2006-02-15 05:34:33.0", currRecord.get("last_update").toString());
        assertThat(((Date) currRecord.get("last_update")).toInstant()).isEqualTo(df.parse("2006-02-15 04:34:33-00").toInstant());

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("actor_id")).isTrue();
        assertThat(currRecord.get("actor_id")).isEqualTo(2);
        assertThat(currRecord.containsKey("first_name")).isTrue();
        assertThat(currRecord.get("first_name")).isEqualTo("NICK");
        assertThat(currRecord.containsKey("last_name")).isTrue();
        assertThat(currRecord.get("last_name")).isEqualTo("WAHLBERG");
        assertThat(currRecord.containsKey("last_update")).isTrue();
        //        assertEquals("2006-02-15 05:34:33.0", currRecord.get("last_update").toString());
        assertThat(((Date) currRecord.get("last_update")).toInstant()).isEqualTo(df.parse("2006-02-15 04:34:33-00").toInstant());

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("actor_id")).isTrue();
        assertThat(currRecord.get("actor_id")).isEqualTo(3);
        assertThat(currRecord.containsKey("first_name")).isTrue();
        assertThat(currRecord.get("first_name")).isEqualTo("ED");
        assertThat(currRecord.containsKey("last_name")).isTrue();
        assertThat(currRecord.get("last_name")).isEqualTo("CHASE");
        assertThat(currRecord.containsKey("last_update")).isTrue();
        //        assertEquals("2006-02-15 05:34:33.0", currRecord.get("last_update").toString());
        assertThat(((Date) currRecord.get("last_update")).toInstant()).isEqualTo(df.parse("2006-02-15 04:34:33-00").toInstant());

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("actor_id")).isTrue();
        assertThat(currRecord.get("actor_id")).isEqualTo(4);
        assertThat(currRecord.containsKey("first_name")).isTrue();
        assertThat(currRecord.get("first_name")).isEqualTo("JENNIFER");
        assertThat(currRecord.containsKey("last_name")).isTrue();
        assertThat(currRecord.get("last_name")).isEqualTo("DAVIS");
        assertThat(currRecord.containsKey("last_update")).isTrue();
        //        assertEquals("2006-02-15 05:34:33.0", currRecord.get("last_update").toString());
        assertThat(((Date) currRecord.get("last_update")).toInstant()).isEqualTo(df.parse("2006-02-15 04:34:33-00").toInstant());

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("actor_id")).isTrue();
        assertThat(currRecord.get("actor_id")).isEqualTo(5);
        assertThat(currRecord.containsKey("first_name")).isTrue();
        assertThat(currRecord.get("first_name")).isEqualTo("JOHNNY");
        assertThat(currRecord.containsKey("last_name")).isTrue();
        assertThat(currRecord.get("last_name")).isEqualTo("LOLLOBRIGIDA");
        assertThat(currRecord.containsKey("last_update")).isTrue();
        //        assertEquals("2006-02-15 05:34:33.0", currRecord.get("last_update").toString());
        assertThat(((Date) currRecord.get("last_update")).toInstant()).isEqualTo(df.parse("2006-02-15 04:34:33-00").toInstant());
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

        String[] ids = { "1_1", "1_2", "1_3", "1_4", "1_5", "16_1", "16_2" };

        GraphData data = provider.load(dataSource, ids);

        assertThat(data.getNodesClasses().size()).isEqualTo(2);
        assertThat(data.getNodes().size()).isEqualTo(7);
        assertThat(data.getEdgesClasses().size()).isEqualTo(0);
        assertThat(data.getEdges().size()).isEqualTo(0);

        // Node classes checks
        assertThat(data.getNodesClasses().containsKey("actor")).isTrue();
        assertThat(data.getNodesClasses().containsKey("store")).isTrue();

        Map<String, Object> actorClass = data.getNodesClasses().get("actor");
        assertThat(actorClass.containsKey("actor_id")).isTrue();
        assertThat(actorClass.containsKey("first_name")).isTrue();
        assertThat(actorClass.containsKey("last_name")).isTrue();
        assertThat(actorClass.containsKey("last_update")).isTrue();
        Map<String, Object> storeClass = data.getNodesClasses().get("store");
        assertThat(storeClass.containsKey("store_id")).isTrue();
        assertThat(storeClass.containsKey("manager_staff_id")).isTrue();
        assertThat(storeClass.containsKey("address_id")).isTrue();
        assertThat(storeClass.containsKey("last_update")).isTrue();

        // nodes checks
        Iterator<CytoData> it = data.getNodes().iterator();
        CytoData currNodeContent;
        Map<String, Object> currRecord;

        try {
            currNodeContent = it.next();
            assertThat(currNodeContent.getData()).isNotNull();
            assertThat(currNodeContent.getData().getRecord()).isNotNull();
            currRecord = currNodeContent.getData().getRecord();
            assertThat(currRecord.containsKey("actor_id")).isTrue();
            assertThat(currRecord.get("actor_id")).isEqualTo(1);
            assertThat(currRecord.containsKey("first_name")).isTrue();
            assertThat(currRecord.get("first_name")).isEqualTo("PENELOPE");
            assertThat(currRecord.containsKey("last_name")).isTrue();
            assertThat(currRecord.get("last_name")).isEqualTo("GUINESS");
            assertThat(currRecord.containsKey("last_update")).isTrue();
            assertThat(((Date) currRecord.get("last_update")).toInstant()).isEqualTo(df.parse("2006-02-15 04:34:33-00").toInstant());

            currNodeContent = it.next();
            assertThat(currNodeContent.getData()).isNotNull();
            assertThat(currNodeContent.getData().getRecord()).isNotNull();
            currRecord = currNodeContent.getData().getRecord();
            assertThat(currRecord.containsKey("actor_id")).isTrue();
            assertThat(currRecord.get("actor_id")).isEqualTo(2);
            assertThat(currRecord.containsKey("first_name")).isTrue();
            assertThat(currRecord.get("first_name")).isEqualTo("NICK");
            assertThat(currRecord.containsKey("last_name")).isTrue();
            assertThat(currRecord.get("last_name")).isEqualTo("WAHLBERG");
            assertThat(currRecord.containsKey("last_update")).isTrue();
            assertThat(((Date) currRecord.get("last_update")).toInstant()).isEqualTo(df.parse("2006-02-15 04:34:33-00").toInstant());

            currNodeContent = it.next();
            assertThat(currNodeContent.getData()).isNotNull();
            assertThat(currNodeContent.getData().getRecord()).isNotNull();
            currRecord = currNodeContent.getData().getRecord();
            assertThat(currRecord.containsKey("actor_id")).isTrue();
            assertThat(currRecord.get("actor_id")).isEqualTo(3);
            assertThat(currRecord.containsKey("first_name")).isTrue();
            assertThat(currRecord.get("first_name")).isEqualTo("ED");
            assertThat(currRecord.containsKey("last_name")).isTrue();
            assertThat(currRecord.get("last_name")).isEqualTo("CHASE");
            assertThat(currRecord.containsKey("last_update")).isTrue();
            assertThat(((Date) currRecord.get("last_update")).toInstant()).isEqualTo(df.parse("2006-02-15 04:34:33-00").toInstant());

            currNodeContent = it.next();
            assertThat(currNodeContent.getData()).isNotNull();
            assertThat(currNodeContent.getData().getRecord()).isNotNull();
            currRecord = currNodeContent.getData().getRecord();
            assertThat(currRecord.containsKey("actor_id")).isTrue();
            assertThat(currRecord.get("actor_id")).isEqualTo(4);
            assertThat(currRecord.containsKey("first_name")).isTrue();
            assertThat(currRecord.get("first_name")).isEqualTo("JENNIFER");
            assertThat(currRecord.containsKey("last_name")).isTrue();
            assertThat(currRecord.get("last_name")).isEqualTo("DAVIS");
            assertThat(currRecord.containsKey("last_update")).isTrue();
            assertThat(((Date) currRecord.get("last_update")).toInstant()).isEqualTo(df.parse("2006-02-15 04:34:33-00").toInstant());

            currNodeContent = it.next();
            assertThat(currNodeContent.getData()).isNotNull();
            assertThat(currNodeContent.getData().getRecord()).isNotNull();
            currRecord = currNodeContent.getData().getRecord();
            assertThat(currRecord.containsKey("actor_id")).isTrue();
            assertThat(currRecord.get("actor_id")).isEqualTo(5);
            assertThat(currRecord.containsKey("first_name")).isTrue();
            assertThat(currRecord.get("first_name")).isEqualTo("JOHNNY");
            assertThat(currRecord.containsKey("last_name")).isTrue();
            assertThat(currRecord.get("last_name")).isEqualTo("LOLLOBRIGIDA");
            assertThat(currRecord.containsKey("last_update")).isTrue();
            assertThat(((Date) currRecord.get("last_update")).toInstant()).isEqualTo(df.parse("2006-02-15 04:34:33-00").toInstant());

            currNodeContent = it.next();
            assertThat(currNodeContent.getData()).isNotNull();
            assertThat(currNodeContent.getData().getRecord()).isNotNull();
            currRecord = currNodeContent.getData().getRecord();
            assertThat(currRecord.containsKey("store_id")).isTrue();
            assertThat(currRecord.get("store_id")).isEqualTo(1);
            assertThat(currRecord.containsKey("manager_staff_id")).isTrue();
            assertThat(currRecord.get("manager_staff_id")).isEqualTo(1);
            assertThat(currRecord.containsKey("address_id")).isTrue();
            assertThat(currRecord.get("address_id")).isEqualTo(1);
            assertThat(currRecord.containsKey("last_update")).isTrue();
            assertThat(((Date) currRecord.get("last_update")).toInstant()).isEqualTo(df.parse("2006-02-15 04:57:12-00").toInstant());

            currNodeContent = it.next();
            assertThat(currNodeContent.getData()).isNotNull();
            assertThat(currNodeContent.getData().getRecord()).isNotNull();
            currRecord = currNodeContent.getData().getRecord();
            assertThat(currRecord.containsKey("store_id")).isTrue();
            assertThat(currRecord.get("store_id")).isEqualTo(2);
            assertThat(currRecord.containsKey("manager_staff_id")).isTrue();
            assertThat(currRecord.get("manager_staff_id")).isEqualTo(2);
            assertThat(currRecord.containsKey("address_id")).isTrue();
            assertThat(currRecord.get("address_id")).isEqualTo(2);
            assertThat(currRecord.containsKey("last_update")).isTrue();
            assertThat(((Date) currRecord.get("last_update")).toInstant()).isEqualTo(df.parse("2006-02-15 04:57:12-00").toInstant());
        } catch (ParseException e) {
            e.printStackTrace();
            fail("");
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

        String[] actorIds = { "1_1", "1_2", "1_3", "1_4", "1_5" };

        GraphData data = provider.load(dataSource, actorIds);

        assertThat(data.getNodesClasses().size()).isEqualTo(1);
        assertThat(data.getNodes().size()).isEqualTo(5);
        assertThat(data.getEdgesClasses().size()).isEqualTo(0);
        assertThat(data.getEdges().size()).isEqualTo(0);

        // Node classes checks
        assertThat(data.getNodesClasses().containsKey("actor")).isTrue();

        Map<String, Object> actorClass = data.getNodesClasses().get("actor");
        assertThat(actorClass.containsKey("actor_id")).isTrue();
        assertThat(actorClass.containsKey("first_name")).isTrue();
        assertThat(actorClass.containsKey("last_name")).isTrue();
        assertThat(actorClass.containsKey("last_update")).isTrue();

        // nodes checks
        Iterator<CytoData> it = data.getNodes().iterator();
        CytoData currNodeContent;
        Map<String, Object> currRecord;

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("actor_id")).isTrue();
        assertThat(currRecord.get("actor_id")).isEqualTo(1);
        assertThat(currRecord.containsKey("first_name")).isTrue();
        assertThat(currRecord.get("first_name")).isEqualTo("PENELOPE");
        assertThat(currRecord.containsKey("last_name")).isTrue();
        assertThat(currRecord.get("last_name")).isEqualTo("GUINESS");
        assertThat(currRecord.containsKey("last_update")).isTrue();

        assertThat(((Date) currRecord.get("last_update")).toInstant()).isEqualTo(df.parse("2006-02-15 04:34:33-00").toInstant());

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("actor_id")).isTrue();
        assertThat(currRecord.get("actor_id")).isEqualTo(2);
        assertThat(currRecord.containsKey("first_name")).isTrue();
        assertThat(currRecord.get("first_name")).isEqualTo("NICK");
        assertThat(currRecord.containsKey("last_name")).isTrue();
        assertThat(currRecord.get("last_name")).isEqualTo("WAHLBERG");
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(((Date) currRecord.get("last_update")).toInstant()).isEqualTo(df.parse("2006-02-15 04:34:33-00").toInstant());

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("actor_id")).isTrue();
        assertThat(currRecord.get("actor_id")).isEqualTo(3);
        assertThat(currRecord.containsKey("first_name")).isTrue();
        assertThat(currRecord.get("first_name")).isEqualTo("ED");
        assertThat(currRecord.containsKey("last_name")).isTrue();
        assertThat(currRecord.get("last_name")).isEqualTo("CHASE");
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(((Date) currRecord.get("last_update")).toInstant()).isEqualTo(df.parse("2006-02-15 04:34:33-00").toInstant());

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("actor_id")).isTrue();
        assertThat(currRecord.get("actor_id")).isEqualTo(4);
        assertThat(currRecord.containsKey("first_name")).isTrue();
        assertThat(currRecord.get("first_name")).isEqualTo("JENNIFER");
        assertThat(currRecord.containsKey("last_name")).isTrue();
        assertThat(currRecord.get("last_name")).isEqualTo("DAVIS");
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(((Date) currRecord.get("last_update")).toInstant()).isEqualTo(df.parse("2006-02-15 04:34:33-00").toInstant());

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("actor_id")).isTrue();
        assertThat(currRecord.get("actor_id")).isEqualTo(5);
        assertThat(currRecord.containsKey("first_name")).isTrue();
        assertThat(currRecord.get("first_name")).isEqualTo("JOHNNY");
        assertThat(currRecord.containsKey("last_name")).isTrue();
        assertThat(currRecord.get("last_name")).isEqualTo("LOLLOBRIGIDA");
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(((Date) currRecord.get("last_update")).toInstant()).isEqualTo(df.parse("2006-02-15 04:34:33-00").toInstant());

        /*
         * Fetching 2 vertices from the store table by cyto-ids
         * cyto-id from is x_y where:
         *  - x is refers to the source table (actor is the first table in the source db schema)
         *  - y is the external key, then the pk value of the source record
         */

        String[] storeIds = { "16_1", "16_2" };
        data = provider.load(dataSource, storeIds);

        assertThat(data.getNodesClasses().size()).isEqualTo(1);
        assertThat(data.getNodes().size()).isEqualTo(2);
        assertThat(data.getEdgesClasses().size()).isEqualTo(0);
        assertThat(data.getEdges().size()).isEqualTo(0);

        // Node classes checks
        assertThat(data.getNodesClasses().containsKey("store")).isTrue();

        Map<String, Object> storeClass = data.getNodesClasses().get("store");
        assertThat(storeClass.containsKey("store_id")).isTrue();
        assertThat(storeClass.containsKey("manager_staff_id")).isTrue();
        assertThat(storeClass.containsKey("address_id")).isTrue();
        assertThat(storeClass.containsKey("last_update")).isTrue();

        // nodes checks
        it = data.getNodes().iterator();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("store_id")).isTrue();
        assertThat(currRecord.get("store_id")).isEqualTo(1);
        assertThat(currRecord.containsKey("manager_staff_id")).isTrue();
        assertThat(currRecord.get("manager_staff_id")).isEqualTo(1);
        assertThat(currRecord.containsKey("address_id")).isTrue();
        assertThat(currRecord.get("address_id")).isEqualTo(1);
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(df.parse("2006-02-15 04:57:12-00").toInstant()).isEqualTo(((Date) currRecord.get("last_update")).toInstant());

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("store_id")).isTrue();
        assertThat(currRecord.get("store_id")).isEqualTo(2);
        assertThat(currRecord.containsKey("manager_staff_id")).isTrue();
        assertThat(currRecord.get("manager_staff_id")).isEqualTo(2);
        assertThat(currRecord.containsKey("address_id")).isTrue();
        assertThat(currRecord.get("address_id")).isEqualTo(2);
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(df.parse("2006-02-15 04:57:12-00").toInstant()).isEqualTo(((Date) currRecord.get("last_update")).toInstant());
    }

    @Override
    @Test
    public void expand1To1RelWithSimpleFKTest() throws Exception {
        // 1-1 Relationship: distribution_country -[has_distribution_language]-> language

        // adding schema info
        Connection connection = null;
        Statement st = null;

        try {
            connection =
                DriverManager.getConnection(
                    MySQLContainerHolder.container.getJdbcUrl(),
                    MySQLContainerHolder.container.getUsername(),
                    MySQLContainerHolder.container.getPassword()
                );

            String distributioCountryTableBuilding =
                "create table distribution_country (distribution_country_id integer not null," +
                " name varchar(256) not null, distribution_language tinyint unsigned, primary key (distribution_country_id)," +
                " foreign key (distribution_language) references language(language_id)) ENGINE=INNODB";
            st = connection.createStatement();
            st.execute(distributioCountryTableBuilding);

            // populating the new table
            String distributionCountryFilling =
                "insert into distribution_country (distribution_country_id,name,distribution_language) values " +
                "(1,'USA',1)," +
                "(2,'Italy',2)," +
                "(3,'Japan',3)," +
                "(4,'China',4)," +
                "(5,'France',5)," +
                "(6,'Germany',6)";
            st.execute(distributionCountryFilling);
        } catch (Exception e) {
            e.printStackTrace();
            fail("");
        } finally {
            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
                fail("");
            }
        }

        String[] rootIds = { "7_1", "7_2", "7_3", "7_4", "7_5", "7_6" };
        GraphData data = provider.expand(dataSource, rootIds, "out", "has_distribution_language", 300);

        assertThat(data.getNodesClasses().size()).isEqualTo(1);
        assertThat(data.getNodes().size()).isEqualTo(6);
        assertThat(data.getEdgesClasses().size()).isEqualTo(1);
        assertThat(data.getEdges().size()).isEqualTo(6);

        // Node classes checks
        assertThat(data.getNodesClasses().containsKey("language")).isTrue();

        Map<String, Object> languageClass = data.getNodesClasses().get("language");
        assertThat(languageClass.containsKey("language_id")).isTrue();
        assertThat(languageClass.containsKey("name")).isTrue();
        assertThat(languageClass.containsKey("last_update")).isTrue();

        // nodes checks
        Iterator<CytoData> it = data.getNodes().iterator();
        CytoData currNodeContent;
        Map<String, Object> currRecord;

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("language_id")).isTrue();
        assertThat(currRecord.get("language_id")).isEqualTo(1);
        assertThat(currRecord.containsKey("name")).isTrue();
        assertThat(((String) currRecord.get("name")).trim()).isEqualTo("English");
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(df.parse("2006-02-15 05:02:19-00").toInstant()).isEqualTo(((Date) currRecord.get("last_update")).toInstant());

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("language_id")).isTrue();
        assertThat(currRecord.get("language_id")).isEqualTo(2);
        assertThat(currRecord.containsKey("name")).isTrue();
        assertThat(((String) currRecord.get("name")).trim()).isEqualTo("Italian");
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(df.parse("2006-02-15 05:02:19-00").toInstant()).isEqualTo(((Date) currRecord.get("last_update")).toInstant());

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("language_id")).isTrue();
        assertThat(currRecord.get("language_id")).isEqualTo(3);
        assertThat(currRecord.containsKey("name")).isTrue();
        assertThat(((String) currRecord.get("name")).trim()).isEqualTo("Japanese");
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(df.parse("2006-02-15 05:02:19-00").toInstant()).isEqualTo(((Date) currRecord.get("last_update")).toInstant());

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("language_id")).isTrue();
        assertThat(currRecord.get("language_id")).isEqualTo(4);
        assertThat(currRecord.containsKey("name")).isTrue();
        assertThat(((String) currRecord.get("name")).trim()).isEqualTo("Mandarin");
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(df.parse("2006-02-15 05:02:19-00").toInstant()).isEqualTo(((Date) currRecord.get("last_update")).toInstant());

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("language_id")).isTrue();
        assertThat(currRecord.get("language_id")).isEqualTo(5);
        assertThat(currRecord.containsKey("name")).isTrue();
        assertThat(((String) currRecord.get("name")).trim()).isEqualTo("French");
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(df.parse("2006-02-15 05:02:19-00").toInstant()).isEqualTo(((Date) currRecord.get("last_update")).toInstant());

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("language_id")).isTrue();
        assertThat(currRecord.get("language_id")).isEqualTo(6);
        assertThat(currRecord.containsKey("name")).isTrue();
        assertThat(((String) currRecord.get("name")).trim()).isEqualTo("German");
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(df.parse("2006-02-15 05:02:19-00").toInstant()).isEqualTo(((Date) currRecord.get("last_update")).toInstant());

        // edges checks
        it = data.getEdges().iterator();
        CytoData currEdgeContent;

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_distribution_language");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("7_1");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("13_1");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("71_131");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_distribution_language");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("7_2");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("13_2");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("72_132");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_distribution_language");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("7_3");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("13_3");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("73_133");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_distribution_language");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("7_4");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("13_4");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("74_134");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_distribution_language");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("7_5");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("13_5");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("75_135");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_distribution_language");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("7_6");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("13_6");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("76_136");

        // 1-1 Relationship: language <-[has_distribution_language]- distribution_country

        rootIds[0] = "13_1";
        rootIds[1] = "13_2";
        rootIds[2] = "13_3";
        rootIds[3] = "13_4";
        rootIds[4] = "13_5";
        rootIds[5] = "13_6";
        data = provider.expand(dataSource, rootIds, "in", "has_distribution_language", 300);

        assertThat(data.getNodesClasses().size()).isEqualTo(1);
        assertThat(data.getNodes().size()).isEqualTo(6);
        assertThat(data.getEdgesClasses().size()).isEqualTo(1);
        assertThat(data.getEdges().size()).isEqualTo(6);

        // Node classes checks
        assertThat(data.getNodesClasses().containsKey("distribution_country")).isTrue();

        Map<String, Object> distributionCountryClass = data.getNodesClasses().get("distribution_country");
        assertThat(distributionCountryClass.containsKey("distribution_country_id")).isTrue();
        assertThat(distributionCountryClass.containsKey("name")).isTrue();
        assertThat(distributionCountryClass.containsKey("distribution_language")).isTrue();

        it = data.getNodes().iterator();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("distribution_country_id")).isTrue();
        assertThat(currRecord.get("distribution_country_id")).isEqualTo(1);
        assertThat(currRecord.containsKey("name")).isTrue();
        assertThat(((String) currRecord.get("name"))).isEqualTo("USA");
        assertThat(currRecord.containsKey("distribution_language")).isTrue();
        assertThat(currRecord.get("distribution_language")).isEqualTo(1);

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("distribution_country_id")).isTrue();
        assertThat(currRecord.get("distribution_country_id")).isEqualTo(2);
        assertThat(currRecord.containsKey("name")).isTrue();
        assertThat(((String) currRecord.get("name"))).isEqualTo("Italy");
        assertThat(currRecord.containsKey("distribution_language")).isTrue();
        assertThat(currRecord.get("distribution_language")).isEqualTo(2);

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("distribution_country_id")).isTrue();
        assertThat(currRecord.get("distribution_country_id")).isEqualTo(3);
        assertThat(currRecord.containsKey("name")).isTrue();
        assertThat(((String) currRecord.get("name"))).isEqualTo("Japan");
        assertThat(currRecord.containsKey("distribution_language")).isTrue();
        assertThat(currRecord.get("distribution_language")).isEqualTo(3);

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("distribution_country_id")).isTrue();
        assertThat(currRecord.get("distribution_country_id")).isEqualTo(4);
        assertThat(currRecord.containsKey("name")).isTrue();
        assertThat(((String) currRecord.get("name"))).isEqualTo("China");
        assertThat(currRecord.containsKey("distribution_language")).isTrue();
        assertThat(currRecord.get("distribution_language")).isEqualTo(4);

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("distribution_country_id")).isTrue();
        assertThat(currRecord.get("distribution_country_id")).isEqualTo(5);
        assertThat(currRecord.containsKey("name")).isTrue();
        assertThat(((String) currRecord.get("name"))).isEqualTo("France");
        assertThat(currRecord.containsKey("distribution_language")).isTrue();
        assertThat(currRecord.get("distribution_language")).isEqualTo(5);

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("distribution_country_id")).isTrue();
        assertThat(currRecord.get("distribution_country_id")).isEqualTo(6);
        assertThat(currRecord.containsKey("name")).isTrue();
        assertThat(((String) currRecord.get("name"))).isEqualTo("Germany");
        assertThat(currRecord.containsKey("distribution_language")).isTrue();
        assertThat(currRecord.get("distribution_language")).isEqualTo(6);

        // edges checks
        it = data.getEdges().iterator();

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_distribution_language");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("7_1");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("13_1");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("71_131");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_distribution_language");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("7_2");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("13_2");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("72_132");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_distribution_language");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("7_3");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("13_3");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("73_133");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_distribution_language");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("7_4");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("13_4");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("74_134");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_distribution_language");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("7_5");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("13_5");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("75_135");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_distribution_language");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("7_6");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("13_6");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("76_136");

        // dropping the new added table
        try {
            connection =
                DriverManager.getConnection(
                    MySQLContainerHolder.container.getJdbcUrl(),
                    MySQLContainerHolder.container.getUsername(),
                    MySQLContainerHolder.container.getPassword()
                );

            String deleteDistributionCountryTable = "drop table distribution_country";
            st = connection.createStatement();
            st.execute(deleteDistributionCountryTable);
        } catch (Exception e) {
            e.printStackTrace();
            fail("");
        } finally {
            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
                fail("");
            }
        }
    }

    @Override
    @Test
    public void expand1ToNRelWithSimpleFKTest() {
        // 1-N Relationship: city -[has_country]-> country

        String[] rootIds = { "4_1" };
        GraphData data = provider.expand(dataSource, rootIds, "out", "has_country", 300);

        assertThat(data.getNodesClasses().size()).isEqualTo(1);
        assertThat(data.getNodes().size()).isEqualTo(1);
        assertThat(data.getEdgesClasses().size()).isEqualTo(1);
        assertThat(data.getEdges().size()).isEqualTo(1);

        // Node classes checks
        assertThat(data.getNodesClasses().containsKey("country")).isTrue();

        Map<String, Object> customerClass = data.getNodesClasses().get("country");
        assertThat(customerClass.containsKey("country_id")).isTrue();
        assertThat(customerClass.containsKey("country")).isTrue();
        assertThat(customerClass.containsKey("last_update")).isTrue();

        // nodes checks
        Iterator<CytoData> it = data.getNodes().iterator();
        CytoData currNodeContent;
        Map<String, Object> currRecord;

        try {
            currNodeContent = it.next();
            assertThat(currNodeContent.getData()).isNotNull();
            assertThat(currNodeContent.getData().getRecord()).isNotNull();
            currRecord = currNodeContent.getData().getRecord();
            assertThat(currRecord.containsKey("country_id")).isTrue();
            assertThat(currRecord.get("country_id")).isEqualTo(87);
            assertThat(currRecord.containsKey("country")).isTrue();
            assertThat(currRecord.get("country")).isEqualTo("Spain");
            assertThat(currRecord.containsKey("last_update")).isTrue();
            assertThat(df.parse("2006-02-15 04:44:00-00").toInstant()).isEqualTo(((Date) currRecord.get("last_update")).toInstant());
        } catch (ParseException e) {
            e.printStackTrace();
            fail("");
        }

        // edges checks
        it = data.getEdges().iterator();
        CytoData currEdgeContent;

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);

        assertThat(currEdgeContent.getClasses()).isEqualTo("has_country");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("4_1");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("5_87");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("41_587");

        // 1-N Relationship: city <-[has_country]- country

        rootIds[0] = "5_87";
        data = provider.expand(dataSource, rootIds, "in", "has_country", 300);

        assertThat(data.getNodesClasses().size()).isEqualTo(1);
        assertThat(data.getNodes().size()).isEqualTo(5);
        assertThat(data.getEdgesClasses().size()).isEqualTo(1);
        assertThat(data.getEdges().size()).isEqualTo(5);

        // nodes checks
        it = data.getNodes().iterator();

        try {
            currNodeContent = it.next();
            assertThat(currNodeContent.getData()).isNotNull();
            assertThat(currNodeContent.getData().getRecord()).isNotNull();
            currRecord = currNodeContent.getData().getRecord();
            assertThat(currRecord.containsKey("city_id")).isTrue();
            assertThat(currRecord.get("city_id")).isEqualTo(1);
            assertThat(currRecord.containsKey("city")).isTrue();
            assertThat(currRecord.get("city")).isEqualTo("A Corua (La Corua)");
            assertThat(currRecord.containsKey("country_id")).isTrue();
            assertThat(currRecord.get("country_id")).isEqualTo(87);
            assertThat(currRecord.containsKey("last_update")).isTrue();
            assertThat(df.parse("2006-02-15 04:45:25-00").toInstant()).isEqualTo(((Date) currRecord.get("last_update")).toInstant());

            currNodeContent = it.next();
            assertThat(currNodeContent.getData()).isNotNull();
            assertThat(currNodeContent.getData().getRecord()).isNotNull();
            currRecord = currNodeContent.getData().getRecord();
            assertThat(currRecord.containsKey("city_id")).isTrue();
            assertThat(currRecord.get("city_id")).isEqualTo(146);
            assertThat(currRecord.containsKey("city")).isTrue();
            assertThat(currRecord.get("city")).isEqualTo("Donostia-San Sebastin");
            assertThat(currRecord.containsKey("country_id")).isTrue();
            assertThat(currRecord.get("country_id")).isEqualTo(87);
            assertThat(currRecord.containsKey("last_update")).isTrue();
            assertThat(df.parse("2006-02-15 04:45:25-00").toInstant()).isEqualTo(((Date) currRecord.get("last_update")).toInstant());

            currNodeContent = it.next();
            assertThat(currNodeContent.getData()).isNotNull();
            assertThat(currNodeContent.getData().getRecord()).isNotNull();
            currRecord = currNodeContent.getData().getRecord();
            assertThat(currRecord.containsKey("city_id")).isTrue();
            assertThat(currRecord.get("city_id")).isEqualTo(181);
            assertThat(currRecord.containsKey("city")).isTrue();
            assertThat(currRecord.get("city")).isEqualTo("Gijn");
            assertThat(currRecord.containsKey("country_id")).isTrue();
            assertThat(currRecord.get("country_id")).isEqualTo(87);
            assertThat(currRecord.containsKey("last_update")).isTrue();
            assertThat(df.parse("2006-02-15 04:45:25-00").toInstant()).isEqualTo(((Date) currRecord.get("last_update")).toInstant());

            currNodeContent = it.next();
            assertThat(currNodeContent.getData()).isNotNull();
            assertThat(currNodeContent.getData().getRecord()).isNotNull();
            currRecord = currNodeContent.getData().getRecord();
            assertThat(currRecord.containsKey("city_id")).isTrue();
            assertThat(currRecord.get("city_id")).isEqualTo(388);
            assertThat(currRecord.containsKey("city")).isTrue();
            assertThat(currRecord.get("city")).isEqualTo("Ourense (Orense)");
            assertThat(currRecord.containsKey("country_id")).isTrue();
            assertThat(currRecord.get("country_id")).isEqualTo(87);
            assertThat(currRecord.containsKey("last_update")).isTrue();
            assertThat(df.parse("2006-02-15 04:45:25-00").toInstant()).isEqualTo(((Date) currRecord.get("last_update")).toInstant());

            currNodeContent = it.next();
            assertThat(currNodeContent.getData()).isNotNull();
            assertThat(currNodeContent.getData().getRecord()).isNotNull();
            currRecord = currNodeContent.getData().getRecord();
            assertThat(currRecord.containsKey("city_id")).isTrue();
            assertThat(currRecord.get("city_id")).isEqualTo(459);
            assertThat(currRecord.containsKey("city")).isTrue();
            assertThat(currRecord.get("city")).isEqualTo("Santiago de Compostela");
            assertThat(currRecord.containsKey("country_id")).isTrue();
            assertThat(currRecord.get("country_id")).isEqualTo(87);
            assertThat(currRecord.containsKey("last_update")).isTrue();
            assertThat(df.parse("2006-02-15 04:45:25-00").toInstant()).isEqualTo(((Date) currRecord.get("last_update")).toInstant());
        } catch (ParseException e) {
            e.printStackTrace();
            fail("");
        }

        // edges checks
        it = data.getEdges().iterator();

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_country");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("4_1");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("5_87");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("41_587");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_country");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("4_146");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("5_87");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("4146_587");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_country");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("4_181");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("5_87");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("4181_587");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_country");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("4_388");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("5_87");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("4388_587");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_country");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("4_459");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("5_87");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("4459_587");
    }

    @Override
    @Test
    public void expandMultiple1ToNRelWithSimpleFKTest() {
        // 1-N Relationship: rental -[has_customer]-> customer

        String[] rootIds = { "14_1" };
        GraphData data = provider.expand(dataSource, rootIds, "out", "has_customer", 300);

        assertThat(data.getNodesClasses().size()).isEqualTo(1);
        assertThat(data.getNodes().size()).isEqualTo(1);
        assertThat(data.getEdgesClasses().size()).isEqualTo(1);
        assertThat(data.getEdges().size()).isEqualTo(1);

        // Node classes checks
        assertThat(data.getNodesClasses().containsKey("customer")).isTrue();

        Map<String, Object> customerClass = data.getNodesClasses().get("customer");
        assertThat(customerClass.containsKey("customer_id")).isTrue();
        assertThat(customerClass.containsKey("store_id")).isTrue();
        assertThat(customerClass.containsKey("first_name")).isTrue();
        assertThat(customerClass.containsKey("last_name")).isTrue();
        assertThat(customerClass.containsKey("email")).isTrue();
        assertThat(customerClass.containsKey("address_id")).isTrue();
        assertThat(customerClass.containsKey("create_date")).isTrue();
        assertThat(customerClass.containsKey("last_update")).isTrue();
        assertThat(customerClass.containsKey("active")).isTrue();

        // nodes checks
        Iterator<CytoData> it = data.getNodes().iterator();
        CytoData currNodeContent;
        Map<String, Object> currRecord;

        try {
            currNodeContent = it.next();
            assertThat(currNodeContent.getData()).isNotNull();
            assertThat(currNodeContent.getData().getRecord()).isNotNull();
            currRecord = currNodeContent.getData().getRecord();
            assertThat(currRecord.containsKey("customer_id")).isTrue();
            assertThat(currRecord.get("customer_id")).isEqualTo(130);
            assertThat(currRecord.containsKey("store_id")).isTrue();
            assertThat(currRecord.get("store_id")).isEqualTo(1);
            assertThat(currRecord.containsKey("first_name")).isTrue();
            assertThat(currRecord.get("first_name")).isEqualTo("CHARLOTTE");
            assertThat(currRecord.containsKey("last_name")).isTrue();
            assertThat(currRecord.get("last_name")).isEqualTo("HUNTER");
            assertThat(currRecord.containsKey("email")).isTrue();
            assertThat(currRecord.get("email")).isEqualTo("CHARLOTTE.HUNTER@sakilacustomer.org");
            assertThat(currRecord.containsKey("address_id")).isTrue();
            assertThat(currRecord.get("address_id")).isEqualTo(134);
            assertThat(currRecord.containsKey("create_date")).isTrue();
            assertThat(df.parse("2006-02-14 22:04:36-00").toInstant()).isEqualTo(((Date) currRecord.get("create_date")).toInstant());
            assertThat(currRecord.containsKey("last_update")).isTrue();
            assertThat(df.parse("2006-02-15 04:57:20-00").toInstant()).isEqualTo(((Date) currRecord.get("last_update")).toInstant());
            assertThat(currRecord.containsKey("active")).isTrue();
            assertThat(currRecord.get("active")).isEqualTo(true);
        } catch (ParseException e) {
            fail("");
        }

        // edges checks
        it = data.getEdges().iterator();
        CytoData currEdgeContent;

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);

        assertThat(currEdgeContent.getClasses()).isEqualTo("has_customer");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("14_1");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("6_130");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("141_6130");

        // 1-N Relationship: customer <-[has_customer]- rental

        rootIds[0] = "6_130";
        data = provider.expand(dataSource, rootIds, "in", "has_customer", 300);

        assertThat(data.getNodesClasses().size()).isEqualTo(2);
        assertThat(data.getNodes().size()).isEqualTo(48);
        assertThat(data.getEdgesClasses().size()).isEqualTo(1);
        assertThat(data.getEdges().size()).isEqualTo(48);

        // Node classes checks
        assertThat(data.getNodesClasses().containsKey("rental")).isTrue();
        assertThat(data.getNodesClasses().containsKey("payment")).isTrue(); // has_customer represents also the relationship: Payment -> Customer

        // nodes checks
        Set<CytoData> rentalNodes = data.getNodes().stream().filter(rentalNode -> rentalNode.getClasses().equals("rental")).collect(Collectors.toSet());
        Set<CytoData> paymentNodes = data.getNodes().stream().filter(rentalNode -> rentalNode.getClasses().equals("payment")).collect(Collectors.toSet());

        assertThat(rentalNodes.size()).isEqualTo(24);
        assertThat(paymentNodes.size()).isEqualTo(24);
    }

    @Override
    @Test
    public void expand1ToNRelWithJoinTableAndSimpleFKTest() {
        /**
         * Get movies by actor
         */

        // expanding 1-N relationship: actor <-[has_actor]- film_actor
        String[] rootIds = { "1_1" };
        GraphData data = provider.expand(dataSource, rootIds, "in", "has_actor", 300);

        assertThat(data.getNodesClasses().size()).isEqualTo(1);
        assertThat(data.getNodes().size()).isEqualTo(19);
        assertThat(data.getEdgesClasses().size()).isEqualTo(1);
        assertThat(data.getEdges().size()).isEqualTo(19);

        // Node classes checks
        assertThat(data.getNodesClasses().containsKey("film_actor")).isTrue();

        Map<String, Object> filmActorClass = data.getNodesClasses().get("film_actor");
        assertThat(filmActorClass.containsKey("film_id")).isTrue();
        assertThat(filmActorClass.containsKey("actor_id")).isTrue();
        assertThat(filmActorClass.containsKey("last_update")).isTrue();

        String[] joinRecordRoots = new String[19];
        int i = 0;
        for (CytoData currentData : data.getNodes()) {
            joinRecordRoots[i] = "8_1_" + currentData.getData().getRecord().get("film_id").toString();
            i++;
        }

        // expanding 1-N relationship: film_actor -[has_film] -> film

        data = provider.expand(dataSource, joinRecordRoots, "out", "has_film", 300);

        assertThat(data.getNodesClasses().size()).isEqualTo(1);
        assertThat(data.getNodes().size()).isEqualTo(19);
        assertThat(data.getEdgesClasses().size()).isEqualTo(1);
        assertThat(data.getEdges().size()).isEqualTo(19);

        // Node classes checks
        assertThat(data.getNodesClasses().containsKey("film")).isTrue();

        Map<String, Object> filmClass = data.getNodesClasses().get("film");
        assertThat(filmClass.containsKey("film_id")).isTrue();
        assertThat(filmClass.containsKey("title")).isTrue();
        assertThat(filmClass.containsKey("description")).isTrue();
        assertThat(filmClass.containsKey("release_year")).isTrue();
        assertThat(filmClass.containsKey("language_id")).isTrue();
        assertThat(filmClass.containsKey("original_language_id")).isTrue();
        assertThat(filmClass.containsKey("rental_duration")).isTrue();
        assertThat(filmClass.containsKey("rental_rate")).isTrue();
        assertThat(filmClass.containsKey("length")).isTrue();
        assertThat(filmClass.containsKey("replacement_cost")).isTrue();
        assertThat(filmClass.containsKey("rating")).isTrue();
        assertThat(filmClass.containsKey("last_update")).isTrue();
        assertThat(filmClass.containsKey("special_features")).isTrue();

        // nodes checks
        Iterator<CytoData> it = data.getNodes().iterator();
        CytoData currNodeContent;
        Map<String, Object> currRecord;

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(1);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("ACADEMY DINOSAUR");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(23);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("ANACONDA CONFESSIONS");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(25);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("ANGELS LIFE");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(106);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("BULWORTH COMMANDMENTS");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(140);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("CHEAPER CLYDE");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(166);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("COLOR PHILADELPHIA");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(277);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("ELEPHANT TROJAN");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(361);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("GLEAMING JAWBREAKER");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(438);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("HUMAN GRAFFITI");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(499);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("KING EVOLUTION");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(506);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("LADY STAGE");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(509);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("LANGUAGE COWBOY");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(605);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("MULHOLLAND BEAST");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(635);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("OKLAHOMA JUMANJI");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(749);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("RULES HUMAN");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(832);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("SPLASH GUMP");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(939);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("VERTIGO NORTHWEST");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(970);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("WESTWARD SEABISCUIT");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(980);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("WIZARD COLDBLOODED");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        // edges checks
        it = data.getEdges().iterator();
        CytoData currEdgeContent;

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_film");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("8_1_1");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_1");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("811_71");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_film");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("8_1_23");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_23");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8123_723");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_film");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("8_1_25");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_25");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8125_725");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_film");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("8_1_106");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_106");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("81106_7106");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_film");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("8_1_140");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_140");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("81140_7140");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_film");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("8_1_166");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_166");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("81166_7166");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_film");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("8_1_277");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_277");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("81277_7277");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_film");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("8_1_361");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_361");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("81361_7361");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_film");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("8_1_438");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_438");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("81438_7438");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_film");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("8_1_499");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_499");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("81499_7499");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_film");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("8_1_506");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_506");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("81506_7506");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_film");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("8_1_509");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_509");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("81509_7509");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_film");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("8_1_605");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_605");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("81605_7605");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_film");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("8_1_635");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_635");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("81635_7635");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_film");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("8_1_749");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_749");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("81749_7749");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_film");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("8_1_832");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_832");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("81832_7832");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_film");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("8_1_939");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_939");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("81939_7939");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_film");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("8_1_970");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_970");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("81970_7970");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_film");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("8_1_980");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_980");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("81980_7980");

        /**
         * Get actors by movie
         */

        // expanding 1-N relationship: film <-[has_film]- film_actor

        rootIds[0] = "7_2";
        data = provider.expand(dataSource, rootIds, "in", "has_film", 300);

        assertThat(data.getNodesClasses().size()).isEqualTo(3);
        assertThat(data.getNodes().size()).isEqualTo(8);
        assertThat(data.getEdgesClasses().size()).isEqualTo(1);
        assertThat(data.getEdges().size()).isEqualTo(8);

        // Node classes checks
        assertThat(data.getNodesClasses().containsKey("film_actor")).isTrue();
        assertThat(data.getNodesClasses().containsKey("film_category")).isTrue(); // has_film represents also the relationship: film_category -> film
        assertThat(data.getNodesClasses().containsKey("inventory")).isTrue(); // has_film represents also the relationship: inventory -> film

        filmActorClass = data.getNodesClasses().get("film_actor");
        assertThat(filmActorClass.containsKey("film_id")).isTrue();
        assertThat(filmActorClass.containsKey("actor_id")).isTrue();
        assertThat(filmActorClass.containsKey("last_update")).isTrue();
        filmActorClass = data.getNodesClasses().get("film_category");
        assertThat(filmActorClass.containsKey("film_id")).isTrue();
        assertThat(filmActorClass.containsKey("category_id")).isTrue();
        assertThat(filmActorClass.containsKey("last_update")).isTrue();
        Map<String, Object> inventoryClass = data.getNodesClasses().get("inventory");
        assertThat(inventoryClass.containsKey("inventory_id")).isTrue();
        assertThat(inventoryClass.containsKey("film_id")).isTrue();
        assertThat(inventoryClass.containsKey("store_id")).isTrue();
        assertThat(inventoryClass.containsKey("last_update")).isTrue();

        Set<CytoData> filmActorNodes = data
            .getNodes()
            .stream()
            .filter(filmActorNode -> filmActorNode.getClasses().equals("film_actor"))
            .collect(Collectors.toSet());
        Set<CytoData> filmCategoryNodes = data
            .getNodes()
            .stream()
            .filter(filmCategoryNode -> filmCategoryNode.getClasses().equals("film_category"))
            .collect(Collectors.toSet());
        Set<CytoData> inventoryNodes = data
            .getNodes()
            .stream()
            .filter(inventoryNode -> inventoryNode.getClasses().equals("inventory"))
            .collect(Collectors.toSet());

        assertThat(filmActorNodes.size()).isEqualTo(4);
        assertThat(filmCategoryNodes.size()).isEqualTo(1);
        assertThat(inventoryNodes.size()).isEqualTo(3);

        joinRecordRoots = new String[4];
        i = 0;
        for (CytoData currentData : filmActorNodes) {
            joinRecordRoots[i] = "8_" + currentData.getData().getRecord().get("actor_id").toString() + "_2";
            i++;
        }

        // expanding 1-N relationship: film_actor -[has_actor]-> actor

        data = provider.expand(dataSource, joinRecordRoots, "out", "has_actor", 300);

        assertThat(data.getNodesClasses().size()).isEqualTo(1);
        assertThat(data.getNodes().size()).isEqualTo(4);
        assertThat(data.getEdgesClasses().size()).isEqualTo(1);
        assertThat(data.getEdges().size()).isEqualTo(4);

        // Node classes checks
        assertThat(data.getNodesClasses().containsKey("actor")).isTrue();

        Map<String, Object> actorClass = data.getNodesClasses().get("actor");
        assertThat(actorClass.containsKey("actor_id")).isTrue();
        assertThat(actorClass.containsKey("first_name")).isTrue();
        assertThat(actorClass.containsKey("last_name")).isTrue();
        assertThat(actorClass.containsKey("last_update")).isTrue();

        // nodes checks
        it = data.getNodes().iterator();

        try {
            currNodeContent = it.next();
            assertThat(currNodeContent.getData()).isNotNull();
            assertThat(currNodeContent.getData().getRecord()).isNotNull();
            currRecord = currNodeContent.getData().getRecord();
            assertThat(currRecord.containsKey("actor_id")).isTrue();
            assertThat(currRecord.get("actor_id")).isEqualTo(19);
            assertThat(currRecord.containsKey("first_name")).isTrue();
            assertThat(currRecord.get("first_name")).isEqualTo("BOB");
            assertThat(currRecord.containsKey("last_name")).isTrue();
            assertThat(currRecord.get("last_name")).isEqualTo("FAWCETT");
            assertThat(currRecord.containsKey("last_update")).isTrue();
            assertThat(df.parse("2006-02-15 04:34:33-00").toInstant()).isEqualTo(((Date) currRecord.get("last_update")).toInstant());

            currNodeContent = it.next();
            assertThat(currNodeContent.getData()).isNotNull();
            assertThat(currNodeContent.getData().getRecord()).isNotNull();
            currRecord = currNodeContent.getData().getRecord();
            assertThat(currRecord.containsKey("actor_id")).isTrue();
            assertThat(currRecord.get("actor_id")).isEqualTo(85);
            assertThat(currRecord.containsKey("first_name")).isTrue();
            assertThat(currRecord.get("first_name")).isEqualTo("MINNIE");
            assertThat(currRecord.containsKey("last_name")).isTrue();
            assertThat(currRecord.get("last_name")).isEqualTo("ZELLWEGER");
            assertThat(currRecord.containsKey("last_update")).isTrue();
            assertThat(df.parse("2006-02-15 04:34:33-00").toInstant()).isEqualTo(((Date) currRecord.get("last_update")).toInstant());

            currNodeContent = it.next();
            assertThat(currNodeContent.getData()).isNotNull();
            assertThat(currNodeContent.getData().getRecord()).isNotNull();
            currRecord = currNodeContent.getData().getRecord();
            assertThat(currRecord.containsKey("actor_id")).isTrue();
            assertThat(currRecord.get("actor_id")).isEqualTo(90);
            assertThat(currRecord.containsKey("first_name")).isTrue();
            assertThat(currRecord.get("first_name")).isEqualTo("SEAN");
            assertThat(currRecord.containsKey("last_name")).isTrue();
            assertThat(currRecord.get("last_name")).isEqualTo("GUINESS");
            assertThat(currRecord.containsKey("last_update")).isTrue();
            assertThat(df.parse("2006-02-15 04:34:33-00").toInstant()).isEqualTo(((Date) currRecord.get("last_update")).toInstant());

            currNodeContent = it.next();
            assertThat(currNodeContent.getData()).isNotNull();
            assertThat(currNodeContent.getData().getRecord()).isNotNull();
            currRecord = currNodeContent.getData().getRecord();
            assertThat(currRecord.containsKey("actor_id")).isTrue();
            assertThat(currRecord.get("actor_id")).isEqualTo(160);
            assertThat(currRecord.containsKey("first_name")).isTrue();
            assertThat(currRecord.get("first_name")).isEqualTo("CHRIS");
            assertThat(currRecord.containsKey("last_name")).isTrue();
            assertThat(currRecord.get("last_name")).isEqualTo("DEPP");
            assertThat(currRecord.containsKey("last_update")).isTrue();
            assertThat(df.parse("2006-02-15 04:34:33-00").toInstant()).isEqualTo(((Date) currRecord.get("last_update")).toInstant());
        } catch (ParseException e) {
            e.printStackTrace();
            fail("");
        }

        // edges checks
        it = data.getEdges().iterator();

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("8_19_2");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("1_19");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8192_119");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("8_85_2");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("1_85");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8852_185");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("8_90_2");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("1_90");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8902_190");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(0);
        assertThat(currEdgeContent.getClasses()).isEqualTo("has_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("8_160_2");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("1_160");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("81602_1160");
    }

    @Override
    @Test
    public void expand1To1RelWithCompositeFKTest() {}

    @Override
    @Test
    public void expand1ToNRelWithCompositeFKTest() {}

    @Override
    @Test
    public void expand1ToNRelWithJoinTableAndCompositeFKTest() {}
}
