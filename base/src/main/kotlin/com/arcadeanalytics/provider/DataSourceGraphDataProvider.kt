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
package com.arcadeanalytics.provider

/**
 * Interface to be implemented by specialized data providers
 *  @author Roberto Franchini
 */
interface DataSourceGraphDataProvider : DataSourceProvider {

    fun testConnection(dataSource: DataSourceInfo): Boolean = true


    fun fetchData(dataSource: DataSourceInfo,
                  query: String,
                  limit: Int): GraphData

    fun expand(dataSource: DataSourceInfo,
               ids: Array<String>,
               direction: String,
               edgeLabel: String,
               maxTraversal: Int): GraphData

    fun load(dataSource: DataSourceInfo,
             ids: Array<String>): GraphData

    fun loadFromClass(dataSource: DataSourceInfo,
                      className: String,
                      limit: Int): GraphData

    fun loadFromClass(dataSource: DataSourceInfo,
                      className: String,
                      propName: String,
                      propValue: String,
                      limit: Int): GraphData
}
