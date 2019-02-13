package com.arcadeanalytics.provider.rdbms.factory;

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

import com.arcadeanalytics.provider.rdbms.exception.RDBMSProviderIOException;
import com.arcadeanalytics.provider.rdbms.strategy.WorkflowStrategy;
import com.arcadeanalytics.provider.rdbms.strategy.rdbms.DBMSModelBuildingAggregationStrategy;
import com.arcadeanalytics.provider.rdbms.strategy.rdbms.DBMSSimpleModelBuildingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory used to instantiate the chosen strategy for the importing phase starting from its name.
 *
 * @author Gabriele Ponzi
*/

public class StrategyFactory {

    private final Logger log = LoggerFactory.getLogger(StrategyFactory.class);

    public StrategyFactory() {
    }

    public WorkflowStrategy buildStrategy(String chosenStrategy) throws
            RDBMSProviderIOException {

        WorkflowStrategy strategy = null;

        // choosing strategy for migration from RDBSs

        if (chosenStrategy == null) {
            strategy = new DBMSSimpleModelBuildingStrategy();
        } else {
            switch (chosenStrategy) {

                case "interactive":
                    strategy = new DBMSSimpleModelBuildingStrategy();
                    break;

                case "interactive-aggr":
                    strategy = new DBMSModelBuildingAggregationStrategy();
                    break;

                default:
                    log.error("The typed strategy doesn't exist for migration from the chosen RDBMS.\n");
            }

        }

        if (strategy == null)
            throw new RDBMSProviderIOException("Strategy not available for the chosen source.");

        return strategy;
    }

}
