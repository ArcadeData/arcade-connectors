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

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

public abstract class PostgreSQLContainerHolder {

  public static final PostgreSQLContainer container;

  static {
    container =
        new PostgreSQLContainer(
                DockerImageName.parse("arcadeanalytics/postgres-dvdrental")
                    .asCompatibleSubstituteFor("postgres"))
            .withUsername("postgres")
            .withPassword("postgres");
    container.start();

    container.withDatabaseName("dvdrental");
  }
}
