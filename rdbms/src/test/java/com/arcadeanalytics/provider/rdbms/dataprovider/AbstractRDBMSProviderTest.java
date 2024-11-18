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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class AbstractRDBMSProviderTest {

  protected RDBMSDataProvider provider;

  @BeforeEach
  public abstract void setUp() throws Exception;

  @Test
  public abstract void fetchDataThroughTableScanTest() throws Exception;

  @Test
  public abstract void loadVerticesFromIdsSingleTableTest() throws Exception;

  @Test
  public abstract void loadVerticesFromIdsMultipleTablesTest();

  @Test
  public abstract void expand1To1RelWithSimpleFKTest() throws Exception;

  @Test
  public abstract void expand1ToNRelWithSimpleFKTest();

  @Test
  public abstract void expandMultiple1ToNRelWithSimpleFKTest();

  @Test
  public abstract void expand1ToNRelWithJoinTableAndSimpleFKTest();

  @Test
  public abstract void expand1To1RelWithCompositeFKTest();

  @Test
  public abstract void expand1ToNRelWithCompositeFKTest();

  @Test
  public abstract void expand1ToNRelWithJoinTableAndCompositeFKTest();
}
