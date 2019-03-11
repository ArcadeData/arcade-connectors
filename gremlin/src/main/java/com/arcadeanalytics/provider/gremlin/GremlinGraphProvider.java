package com.arcadeanalytics.provider.gremlin;

/*-
 * #%L
 * Arcade Data
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

import com.arcadeanalytics.data.Sprite;
import com.arcadeanalytics.data.SpritePlayer;
import com.arcadeanalytics.provider.DataSourceGraphProvider;
import com.arcadeanalytics.provider.DataSourceInfo;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.regex.Pattern;

import static com.arcadeanalytics.provider.IndexConstants.*;
import static org.apache.commons.lang3.StringUtils.removeStart;

public class GremlinGraphProvider implements DataSourceGraphProvider {

    private final Logger log = LoggerFactory.getLogger(GremlinGraphProvider.class);

    private final Pattern allFields;

    public GremlinGraphProvider() {
        allFields = Pattern.compile(".*");
    }

    @Override
    public void provideTo(DataSourceInfo dataSource, SpritePlayer player) {

        Cluster cluster = GremlinUtilKt.getCluster(dataSource);

        Client client = cluster.connect().init();
        try {

            provideNodes(dataSource, player, client);

            provideEdges(dataSource, player, client);

            client.close();

        } finally {
            cluster.close();
            client.close();
            player.end();

        }
    }

    private void provideNodes(DataSourceInfo dataSource, SpritePlayer processor, Client client) {
        final long nodes = client.submit("g.V().count()").one().getLong();
        long fetched = 0;
        long skip = 0;
        long limit = Math.min(nodes, 1000);

        log.info("start indexing of data-source {} - total nodes:: {} ", dataSource.getId(), nodes);
        while (fetched < nodes) {

            final ResultSet resultSet = client.submit("g.V().range(" + skip + " , " + limit + ")");

            for (Result r : resultSet) {

                final Vertex element = r.getVertex();

                Sprite sprite = new Sprite();
                element.keys()
                        .stream()
                        .flatMap(key -> IteratorUtils.stream(element.properties(key)))
                        .forEach(v -> sprite.add(v.label(), v.value()));

                sprite.add(ARCADE_ID, dataSource.getId() + "_" + cleanOrientId(element.id().toString()))
                        .add(ARCADE_TYPE, ARCADE_NODE_TYPE)
                        .add("@class", element.label());

                processor.play(sprite);
                fetched++;
            }

            skip = limit;
            limit += 10000;

        }
    }

    private void provideEdges(DataSourceInfo dataSource, SpritePlayer processor, Client client) {
        final long edges = client.submit("g.E().count()").one().getLong();
        long fetched = 0;
        long skip = 0;
        long limit = Math.min(edges, 1000);

        log.info("start indexing of data-source {} - total edges:: {} ", dataSource.getId(), edges);
        while (fetched < edges) {

            final ResultSet resultSet = client.submit("g.E().range(" + skip + " , " + limit + ")");

            for (Result r : resultSet) {

                final Element element = r.getElement();

                if (!element.keys().isEmpty()) {
                    Sprite sprite = new Sprite();
                    element.keys()
                            .forEach(k -> sprite.add(k, element.value(k).toString()));

                    sprite.add(ARCADE_ID, dataSource.getId() + "_" + cleanOrientId(element.id().toString()))
                            .add(ARCADE_TYPE, ARCADE_EDGE_TYPE)
                            .add("@class", element.label());

                    processor.play(sprite);
                }
                fetched++;
            }

            skip = limit;
            limit += 10000;

        }
    }

    @NotNull
    private String cleanOrientId(String id) {
        return removeStart(id, "#")
                .replace(":", "_");
    }

    @NotNull
    @Override
    public Set<String> supportedDataSourceTypes() {
        return Sets.newHashSet("GREMLIN_ORIENTDB", "GREMLIN_NEPTUNE", "GREMLIN_JANUSGRAPH");
    }

}
