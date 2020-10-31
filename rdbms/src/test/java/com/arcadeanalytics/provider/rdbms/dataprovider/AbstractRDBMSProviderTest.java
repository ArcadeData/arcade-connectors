package com.arcadeanalytics.provider.rdbms.dataprovider;

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
