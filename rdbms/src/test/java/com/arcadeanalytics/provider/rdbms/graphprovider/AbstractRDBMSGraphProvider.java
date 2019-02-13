package com.arcadeanalytics.provider.rdbms.graphprovider;

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
import com.arcadeanalytics.provider.rdbms.dataprovider.RDBMSGraphProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.arcadeanalytics.provider.IndexConstants.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;


public abstract class AbstractRDBMSGraphProvider {

    public long nodes;
    public long edges;

    protected RDBMSGraphProvider provider;

    SpritePlayer player = new SpritePlayer() {

        private int processed = 0;

        @Override
        public boolean accept(@NotNull Sprite sprite) {
            return true;
        }

        @Override
        public void end() {

        }


        @Override
        public void begin() {

        }

        @Override
        public void play(Sprite document) {
            assertNotNull(document.entries());
            processed++;

            if (document.valueOf(com.arcadeanalytics.provider.IndexConstants.ARCADE_TYPE).equals(com.arcadeanalytics.provider.IndexConstants.ARCADE_NODE_TYPE)) nodes++;
            if (document.valueOf(com.arcadeanalytics.provider.IndexConstants.ARCADE_TYPE).equals(com.arcadeanalytics.provider.IndexConstants.ARCADE_EDGE_TYPE)) edges++;
        }

        @Override
        public long processed() {
            return processed;
        }
    };


    @Test
    public abstract void shouldFetchAllVertexes();

    @Test
    public abstract void shouldFetchAllVertexesExceptJoinTables();


}
