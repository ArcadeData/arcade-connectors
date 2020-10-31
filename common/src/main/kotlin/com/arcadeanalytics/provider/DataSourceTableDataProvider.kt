/*-
 * #%L
 * Arcade Connectors
 * %%
 * Copyright (C) 2018 - 2020 ArcadeData
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
package com.arcadeanalytics.provider

const val TABLE_CLASS = "Table"

data class QueryParam(val name: String, val type: String, val value: String)

typealias QueryParams = List<QueryParam>

inline fun String.prefixIfAbsent(prefix: String): String {
    return if (this.startsWith(prefix)) this else prefix + this
}

/**
 * Interface to be implemented by specialized data providers
 *  @author Roberto Franchini
 */
interface DataSourceTableDataProvider : DataSourceProvider {

    fun fetchData(
        dataSource: DataSourceInfo,
        query: String,
        params: QueryParams,
        limit: Int
    ): GraphData

    fun fetchData(
        dataSource: DataSourceInfo,
        query: String,
        limit: Int
    ): GraphData
}
