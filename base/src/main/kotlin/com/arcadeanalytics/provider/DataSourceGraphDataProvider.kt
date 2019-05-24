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

    /**
     * Test the connection to the given {@link DataSourceInfo}
     * @param dataSource to be tested
     * @return true if the connection works fine, false in other cases
     */
    fun testConnection(dataSource: DataSourceInfo): Boolean = true


    /**
     * Fetch data from the given dataSource using the provided query in the appropriate query language.
     * Result set could be limited passing a limit value.
     *
     *  @param dataSource the data source
     * @param query the query in the appropriate query language (SQL, Cypher, Gremlin)
     * @param limit max number of element to retrieve
     * @return the {@link GraphData} representation of the result set
     */
    fun fetchData(dataSource: DataSourceInfo,
                  query: String,
                  limit: Int): GraphData

    /**
     * Given a list of nodes ids, a direction and an edge label, expand the graph.
     * A max number of traversal step could be passed and it is usually implemented in approximate way.
     *
     * @param dataSource the data spurce
     * @param ids nodes identifiers
     * @param direction out, in or both
     * @param edgeLabel which edge label use to expand the graph
     * @param maxTraversal max number of traversal step
     * @return the {@link GraphData} representation of the result set
     */
    fun expand(dataSource: DataSourceInfo,
               ids: Array<String>,
               direction: String,
               edgeLabel: String,
               maxTraversal: Int): GraphData

    /**
     * Load elements by ids
     * @param dataSource the data source
     * @param ids list of identifiers to be loaded
     * @return the {@link GraphData} representation of the result set
     */
    fun load(dataSource: DataSourceInfo,
             ids: Array<String>): GraphData


    /**
     * Loads element from a given class. A class is a different concept in  different data stores: type, table, label, class.
     * @param dataSource the data source
     * @param className the class name
     * @param limit max number of element to load
     * @return the {@link GraphData} representation of the result set
     */
    fun loadFromClass(dataSource: DataSourceInfo,
                      className: String,
                      limit: Int): GraphData

    /**
     * Loads element from a given class filtering by a property value.
     * A class is a different concept in  different data stores: type, table, label, class.
     * @param dataSource the data source
     * @param className the class name
     * @param propName the property to use in filter
     * @param propValue the property value
     * @param limit max number of element to load
     * @return the {@link GraphData} representation of the result set
     */

    fun loadFromClass(dataSource: DataSourceInfo,
                      className: String,
                      propName: String,
                      propValue: String,
                      limit: Int): GraphData
}
