package com.arcadeanalytics.provider.rdbms.dataprovider;

/*-
 * #%L
 * Arcade Connectors
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

import org.testcontainers.containers.MySQLContainer;

public abstract class MySQLContainerHolder {

    public static final MySQLContainer container;

    static {
        container = new MySQLContainer("arcadeanalytics/mysql-sakila")
                .withUsername("test")
                .withPassword("test")
                .withDatabaseName("sakila");

        container.start();

        container.withDatabaseName("sakila");


    }
}
