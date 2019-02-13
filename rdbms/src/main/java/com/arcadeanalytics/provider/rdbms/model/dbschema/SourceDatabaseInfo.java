package com.arcadeanalytics.provider.rdbms.model.dbschema;

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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents a source database with all its related info for accessing it.
 */

public class SourceDatabaseInfo {

    private final String sourceId;
    private final String sourceDBName;
    private final String sourceRDBMS;
    private final String driverName;
    private final String url;
    private final String username;
    private final String password;
    private final Map connectionProperties;

    private int maxRowsThreshold;

    public SourceDatabaseInfo(String sourceId, String sourceDBName, String sourceRDBMS,
                              String driverName, String url, String username, String password) {
        this(sourceId, sourceDBName, sourceRDBMS, driverName, url, username, password, new LinkedHashMap<String, String>());
    }


    public SourceDatabaseInfo(String sourceId, String sourceDBName, String sourceRDBMS,
                              String driverName, String url, String username, String password, Map<String, String> connectionProperties) {
        this.sourceId = sourceId;
        this.sourceDBName = sourceDBName;
        this.sourceRDBMS = sourceRDBMS;
        this.driverName = driverName;
        this.url = url;
        this.username = username;
        this.password = password;
        this.connectionProperties = connectionProperties;
    }

    public String getSourceId() {
        return this.sourceId;
    }


    public String getSourceDBName() {
        return sourceDBName;
    }

    public String getSourceRDBMS() {
        return sourceRDBMS;
    }

    public String getDriverName() {
        return this.driverName;
    }


    public String getUrl() {
        return this.url;
    }


    public String getUsername() {
        return this.username;
    }


    public String getPassword() {
        return this.password;
    }

    public Map getConnectionProperties() {
        return connectionProperties;
    }

    public int getMaxRowsThreshold() {
        return this.maxRowsThreshold;
    }

    public void setMaxRowsThreshold(int maxRowsThreshold) {
        this.maxRowsThreshold = maxRowsThreshold;
    }

    @Override
    public int hashCode() {
        int result = sourceId.hashCode();
        result = 31 * result + driverName.hashCode();
        result = 31 * result + url.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        SourceDatabaseInfo that = (SourceDatabaseInfo) o;

        if (!sourceId.equals(that.sourceId))
            return false;
        if (!driverName.equals(that.driverName))
            return false;
        return url.equals(that.url);

    }


}
