package com.arcadeanalytics.provider.gremlin;

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

import com.arcadeanalytics.provider.DataSourceInfo;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.orientdb.io.OrientIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;

public class GremlinSerializerFactory {


    @NotNull
    public static MessageSerializer createSerializer(DataSourceInfo dataSource) {

        final String type = dataSource.getType();
        switch (type) {
            case "GREMLIN_ORIENTDB":
                return new GryoMessageSerializerV3d0(GryoMapper.build()
                        .addRegistry(OrientIoRegistry.getInstance()));
            case "GREMLIN_NEPTUNE":
                return new GryoMessageSerializerV3d0();
            case "GREMLIN_JANUSGRAPH":
                return new GryoMessageSerializerV3d0(GryoMapper.build()
                        .addRegistry(JanusGraphIoRegistry.getInstance()));
            case "GREMLIN_COSMOSDB":
                final GraphSONMessageSerializerV1d0 seriallizer = new GraphSONMessageSerializerV1d0();
                Map<String, Object> conf = new HashMap<>();
                conf.put("serializeResultToString", true);
                seriallizer.configure(conf, null);
                return seriallizer;

            default:
                throw new RuntimeException("requested type not implemented yet:: " + type);
        }
    }

}
