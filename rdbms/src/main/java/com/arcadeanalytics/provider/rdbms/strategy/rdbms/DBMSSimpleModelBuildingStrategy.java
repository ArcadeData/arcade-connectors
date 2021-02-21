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
import com.arcadeanalytics.provider.rdbms.mapper.rdbms.ER2GraphMapper;
import com.arcadeanalytics.provider.rdbms.nameresolver.NameResolver;
import com.arcadeanalytics.provider.rdbms.persistence.handler.DBMSDataTypeHandler;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Gabriele Ponzi
 */

public class DBMSSimpleModelBuildingStrategy extends AbstractDBMSModelBuildingStrategy {

    private static final Logger log = LoggerFactory.getLogger(DBMSSimpleModelBuildingStrategy.class);

    public DBMSSimpleModelBuildingStrategy() {}

    @Override
    public ER2GraphMapper createSchemaMapper(
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
        Statistics statistics
    ) {
        ER2GraphMapper mapper = new ER2GraphMapper(
            dataSource,
            includedTables,
            excludedTables,
            queryEngine,
            handler,
            executionStrategy,
            nameResolver,
            statistics
        );

        // Step 1: DataBase schema building
        mapper.buildSourceDatabaseSchema();
        log.debug("{}", mapper.getDataBaseSchema().toString());

        // Step 2: Graph model building
        mapper.buildGraphModel(nameResolver);
        log.debug("{}", mapper.getGraphModel().toString());

        return mapper;
    }
}
