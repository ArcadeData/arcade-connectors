package com.arcadeanalytics.provider.rdbms.factory;

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
import com.arcadeanalytics.provider.rdbms.mapper.rdbms.ER2GraphMapper;
import com.arcadeanalytics.provider.rdbms.mapper.rdbms.Hibernate2GraphMapper;
import com.arcadeanalytics.provider.rdbms.nameresolver.NameResolver;
import com.arcadeanalytics.provider.rdbms.persistence.handler.DBMSDataTypeHandler;
import java.util.List;

/**
 * Factory used to instantiate the chosen 'Mapper' which will be adopted for the source schema
 * building.
 *
 * @author Gabriele Ponzi
 */
public class MapperFactory {

  public ER2GraphMapper buildMapper(
      String chosenMapper,
      DataSourceInfo dataSource,
      String xmlPath,
      List<String> includedTables,
      List<String> excludedTables,
      String executionStrategy,
      DBQueryEngine queryEngine,
      DBMSDataTypeHandler handler,
      NameResolver nameResolver,
      Statistics statistics) {
    switch (chosenMapper) {
      case "hibernate":
        return new Hibernate2GraphMapper(
            dataSource,
            xmlPath,
            includedTables,
            excludedTables,
            queryEngine,
            handler,
            executionStrategy,
            nameResolver,
            statistics);
      default:
        return new ER2GraphMapper(
            dataSource,
            includedTables,
            excludedTables,
            queryEngine,
            handler,
            executionStrategy,
            nameResolver,
            statistics);
    }
  }
}
