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
import java.util.Iterator;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

class PostgreSQLDataProviderWithAggregationTest extends AbstractRDBMSProviderWithAggregationTest {

    private DataSourceInfo dataSource;

    @BeforeEach
    public void setUp() {
        final PostgreSQLContainer container = PostgreSQLContainerHolder.container;

        this.dataSource =
            new DataSourceInfo(
                1L,
                "RDBMS_POSTGRESQL",
                "testDataSource",
                "desc",
                container.getContainerIpAddress(),
                container.getFirstMappedPort(),
                container.getDatabaseName(),
                container.getUsername(),
                container.getPassword(),
                true,
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
    public void fetchDataThroughTableScanTest() {
        try {
            provider.fetchData(dataSource, "select * from film_actor limit 5", 5);
            fail("");
        } catch (Exception e) {
            String message = e.getMessage();
            assertThat(message.contains("Wrong query content: the requested table was aggregated into")).isTrue();
        }
    }

    @Override
    @Test
    public void expandN2NRelationship() {
        /**
         * Get movies by actor
         */

        // expanding 1-N relationship: actor -[film_actor]-> film
        String[] rootIds = { "1_1" };
        GraphData data = provider.expand(dataSource, rootIds, "out", "film_actor", 300);

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
        assertThat(filmClass.containsKey("rental_duration")).isTrue();
        assertThat(filmClass.containsKey("rental_rate")).isTrue();
        assertThat(filmClass.containsKey("length")).isTrue();
        assertThat(filmClass.containsKey("replacement_cost")).isTrue();
        assertThat(filmClass.containsKey("rating")).isTrue();
        assertThat(filmClass.containsKey("last_update")).isTrue();
        assertThat(filmClass.containsKey("special_features")).isTrue();
        assertThat(filmClass.containsKey("fulltext")).isTrue();

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
        assertThat(currRecord.get("title")).isEqualTo("Academy Dinosaur");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(23);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("Anaconda Confessions");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(25);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("Angels Life");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(106);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("Bulworth Commandments");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(140);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("Cheaper Clyde");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(166);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("Color Philadelphia");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(277);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("Elephant Trojan");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(361);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("Gleaming Jawbreaker");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(438);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("Human Graffiti");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(499);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("King Evolution");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(506);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("Lady Stage");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(509);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("Language Cowboy");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(605);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("Mulholland Beast");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(635);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("Oklahoma Jumanji");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(749);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("Rules Human");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(832);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("Splash Gump");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(939);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("Vertigo Northwest");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(970);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("Westward Seabiscuit");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("film_id")).isTrue();
        assertThat(currRecord.get("film_id")).isEqualTo(980);
        assertThat(currRecord.containsKey("title")).isTrue();
        assertThat(currRecord.get("title")).isEqualTo("Wizard Coldblooded");
        assertThat(currRecord.containsKey("description")).isTrue();
        assertThat(currRecord.get("description")).isNotNull();

        // edges checks
        it = data.getEdges().iterator();
        CytoData currEdgeContent;

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(1);
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currEdgeContent.getClasses()).isEqualTo("film_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("1_1");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_1");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8_1_1");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(1);
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currEdgeContent.getClasses()).isEqualTo("film_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("1_1");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_23");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8_1_23");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(1);
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currEdgeContent.getClasses()).isEqualTo("film_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("1_1");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_25");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8_1_25");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(1);
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currEdgeContent.getClasses()).isEqualTo("film_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("1_1");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_106");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8_1_106");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(1);
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currEdgeContent.getClasses()).isEqualTo("film_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("1_1");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_140");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8_1_140");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(1);
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currEdgeContent.getClasses()).isEqualTo("film_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("1_1");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_166");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8_1_166");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(1);
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currEdgeContent.getClasses()).isEqualTo("film_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("1_1");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_277");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8_1_277");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(1);
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currEdgeContent.getClasses()).isEqualTo("film_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("1_1");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_361");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8_1_361");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(1);
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currEdgeContent.getClasses()).isEqualTo("film_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("1_1");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_438");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8_1_438");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(1);
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currEdgeContent.getClasses()).isEqualTo("film_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("1_1");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_499");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8_1_499");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(1);
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currEdgeContent.getClasses()).isEqualTo("film_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("1_1");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_506");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8_1_506");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(1);
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currEdgeContent.getClasses()).isEqualTo("film_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("1_1");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_509");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8_1_509");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(1);
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currEdgeContent.getClasses()).isEqualTo("film_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("1_1");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_605");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8_1_605");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(1);
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currEdgeContent.getClasses()).isEqualTo("film_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("1_1");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_635");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8_1_635");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(1);
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currEdgeContent.getClasses()).isEqualTo("film_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("1_1");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_749");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8_1_749");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(1);
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currEdgeContent.getClasses()).isEqualTo("film_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("1_1");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_832");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8_1_832");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(1);
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currEdgeContent.getClasses()).isEqualTo("film_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("1_1");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_939");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8_1_939");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(1);
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currEdgeContent.getClasses()).isEqualTo("film_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("1_1");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_970");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8_1_970");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(1);
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currEdgeContent.getClasses()).isEqualTo("film_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("1_1");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_980");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8_1_980");

        /**
         * Get actors by movie
         */

        // expanding 1-N relationship: film <-[has_film]- actor

        rootIds[0] = "7_2";
        data = provider.expand(dataSource, rootIds, "in", "film_actor", 300);

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

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("actor_id")).isTrue();
        assertThat(currRecord.get("actor_id")).isEqualTo(19);
        assertThat(currRecord.containsKey("first_name")).isTrue();
        assertThat(currRecord.get("first_name")).isEqualTo("Bob");
        assertThat(currRecord.containsKey("last_name")).isTrue();
        assertThat(currRecord.get("last_name")).isEqualTo("Fawcett");
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currRecord.get("last_update").toString()).isEqualTo("2013-05-26 14:47:57.62");

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("actor_id")).isTrue();
        assertThat(currRecord.get("actor_id")).isEqualTo(85);
        assertThat(currRecord.containsKey("first_name")).isTrue();
        assertThat(currRecord.get("first_name")).isEqualTo("Minnie");
        assertThat(currRecord.containsKey("last_name")).isTrue();
        assertThat(currRecord.get("last_name")).isEqualTo("Zellweger");
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currRecord.get("last_update").toString()).isEqualTo("2013-05-26 14:47:57.62");

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("actor_id")).isTrue();
        assertThat(currRecord.get("actor_id")).isEqualTo(90);
        assertThat(currRecord.containsKey("first_name")).isTrue();
        assertThat(currRecord.get("first_name")).isEqualTo("Sean");
        assertThat(currRecord.containsKey("last_name")).isTrue();
        assertThat(currRecord.get("last_name")).isEqualTo("Guiness");
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currRecord.get("last_update").toString()).isEqualTo("2013-05-26 14:47:57.62");

        currNodeContent = it.next();
        assertThat(currNodeContent.getData()).isNotNull();
        assertThat(currNodeContent.getData().getRecord()).isNotNull();
        currRecord = currNodeContent.getData().getRecord();
        assertThat(currRecord.containsKey("actor_id")).isTrue();
        assertThat(currRecord.get("actor_id")).isEqualTo(160);
        assertThat(currRecord.containsKey("first_name")).isTrue();
        assertThat(currRecord.get("first_name")).isEqualTo("Chris");
        assertThat(currRecord.containsKey("last_name")).isTrue();
        assertThat(currRecord.get("last_name")).isEqualTo("Depp");
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currRecord.get("last_update").toString()).isEqualTo("2013-05-26 14:47:57.62");

        // edges checks
        it = data.getEdges().iterator();

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(1);
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currEdgeContent.getClasses()).isEqualTo("film_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("1_19");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_2");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8_19_2");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(1);
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currEdgeContent.getClasses()).isEqualTo("film_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("1_85");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_2");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8_85_2");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(1);
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currEdgeContent.getClasses()).isEqualTo("film_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("1_90");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_2");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8_90_2");

        currEdgeContent = it.next();
        assertThat(currEdgeContent.getData()).isNotNull();
        assertThat(currEdgeContent.getData().getRecord()).isNotNull();
        currRecord = currEdgeContent.getData().getRecord();
        assertThat(currRecord.size()).isEqualTo(1);
        assertThat(currRecord.containsKey("last_update")).isTrue();
        assertThat(currEdgeContent.getClasses()).isEqualTo("film_actor");
        assertThat(currEdgeContent.getGroup()).isEqualTo("edges");
        assertThat(currEdgeContent.getData().getSource()).isEqualTo("1_160");
        assertThat(currEdgeContent.getData().getTarget()).isEqualTo("7_2");
        assertThat(currEdgeContent.getData().getId()).isEqualTo("8_160_2");
    }
}
