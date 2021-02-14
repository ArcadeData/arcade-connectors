package com.arcadeanalytics.provider.rdbms.strategy;

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

import com.arcadeanalytics.provider.DataSourceInfo;
import com.arcadeanalytics.provider.rdbms.context.Statistics;
import com.arcadeanalytics.provider.rdbms.dbengine.DBQueryEngine;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.List;

/**
 * Interface that represents a specific approach of data importing.
 *
 * @author Gabriele Ponzi
 */

public interface WorkflowStrategy {
  void executeStrategy(
    DataSourceInfo dataSource,
    String outOrientGraphUri,
    String chosenMapper,
    String xmlPath,
    String nameResolverConvention,
    List<String> includedTables,
    List<String> excludedTables,
    ODocument migrationConfig,
    String executionStrategy,
    DBQueryEngine queryEngine,
    Statistics statistics
  );
}
