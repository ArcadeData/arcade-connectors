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
package com.arcadeanalytics.provider

data class Position(private val x: Double, private val y: Double)

data class Data(
    val id: String,
    val parent: String = "",
    val source: String = "",
    val target: String = "",
    val record: MutableMap<String, Any> = mutableMapOf()
)

data class CytoData(
    val classes: String,
    val group: String,
    val data: Data,
    var position: Position = Position(0.0, 0.0),
    val selected: String = "",
    val selectable: String = "",
    val locked: String = "",
    val grabbable: String = ""
)

data class GraphData(
    val nodesClasses: Map<String, Map<String, Any>> = emptyMap(),
    val edgesClasses: Map<String, Map<String, Any>> = emptyMap(),
    val nodes: Set<CytoData>,
    val edges: Set<CytoData>,
    val truncated: Boolean = false
) {

    companion object {

        /**
         * Null object
         */
        @JvmStatic
        val EMPTY = GraphData(
            emptyMap(),
            emptyMap(),
            emptySet(),
            emptySet(),
            false
        )
    }
}
