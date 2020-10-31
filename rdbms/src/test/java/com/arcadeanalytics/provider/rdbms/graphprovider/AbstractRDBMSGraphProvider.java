package com.arcadeanalytics.provider.rdbms.graphprovider;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.arcadeanalytics.data.Sprite;
import com.arcadeanalytics.data.SpritePlayer;
import com.arcadeanalytics.provider.rdbms.dataprovider.RDBMSGraphProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

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
    public void end() {}

    @Override
    public void begin() {}

    @Override
    public void play(Sprite document) {
      assertNotNull(document.entries());
      processed++;

      if (
        document.valueOf(com.arcadeanalytics.provider.IndexConstants.ARCADE_TYPE).equals(com.arcadeanalytics.provider.IndexConstants.ARCADE_NODE_TYPE)
      ) nodes++;
      if (
        document.valueOf(com.arcadeanalytics.provider.IndexConstants.ARCADE_TYPE).equals(com.arcadeanalytics.provider.IndexConstants.ARCADE_EDGE_TYPE)
      ) edges++;
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
