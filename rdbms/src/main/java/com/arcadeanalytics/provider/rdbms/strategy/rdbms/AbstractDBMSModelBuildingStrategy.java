package com.arcadeanalytics.provider.rdbms.strategy.rdbms;

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
import com.arcadeanalytics.provider.rdbms.factory.DataTypeHandlerFactory;
import com.arcadeanalytics.provider.rdbms.factory.NameResolverFactory;
import com.arcadeanalytics.provider.rdbms.mapper.rdbms.ER2GraphMapper;
import com.arcadeanalytics.provider.rdbms.nameresolver.NameResolver;
import com.arcadeanalytics.provider.rdbms.persistence.handler.DBMSDataTypeHandler;
import com.arcadeanalytics.provider.rdbms.strategy.WorkflowStrategy;
import com.arcadeanalytics.provider.rdbms.util.FunctionsHandler;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Date;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Gabriele Ponzi
 */
public abstract class AbstractDBMSModelBuildingStrategy implements WorkflowStrategy {

  private final Logger log = LoggerFactory.getLogger(AbstractDBMSModelBuildingStrategy.class);

  protected ER2GraphMapper mapper;

  public AbstractDBMSModelBuildingStrategy() {}

  @Override
  public void executeStrategy(
      DataSourceInfo dataSource,
      String outOrientGraphUri,
      String chosenMapper,
      String xmlPath,
      String nameResolverConvention,
      List<String> includedTables,
      List<String> excludedTables,
      ODocument migrationConfigDoc,
      String executionStrategy,
      DBQueryEngine queryEngine,
      Statistics statistics) {
    Date globalStart = new Date();

    DataTypeHandlerFactory dataTypeHandlerFactory = new DataTypeHandlerFactory();
    DBMSDataTypeHandler handler =
        (DBMSDataTypeHandler) dataTypeHandlerFactory.buildDataTypeHandler(dataSource.getType());

    /*
     * Step 1,2
     */

    NameResolverFactory nameResolverFactory = new NameResolverFactory();
    NameResolver nameResolver = nameResolverFactory.buildNameResolver(nameResolverConvention);

    this.mapper =
        this.createSchemaMapper(
            dataSource,
            outOrientGraphUri,
            chosenMapper,
            xmlPath,
            nameResolver,
            handler,
            includedTables,
            excludedTables,
            executionStrategy,
            queryEngine,
            statistics);

    Date globalEnd = new Date();

    log.info(
        "Graph model building complete in {}",
        FunctionsHandler.getHMSFormat(globalStart, globalEnd));
  }

  public abstract ER2GraphMapper createSchemaMapper(
      DataSourceInfo dataSource,
      String outOrientGraphUri,
      String chosenMapper,
      String xmlPath,
      NameResolver nameResolver,
      DBMSDataTypeHandler handler,
      List<String> includedTables,
      List<String> excludedTables,
      String executionStrategy,
      DBQueryEngine queryEngine,
      Statistics statistics);
}
