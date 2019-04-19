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
package com.arcadeanalytics.provider.orient3

import com.orientechnologies.orient.core.command.OCommandResultListener
import com.orientechnologies.orient.core.record.impl.ODocument

internal class OrientDB3ResultListener(private val documentCollector: OrientDB3DocumentCollector,
                                       private val limit: Int)
    : OCommandResultListener {

    //state
    private var nodes: Int = 0
    private var edges: Int = 0

    override fun result(record: Any): Boolean {

        //it is an ODocument
        val document = record as ODocument

        documentCollector.collect(document)

        when {
            document.isVertexType() -> nodes++
            document.isEdgeType() -> edges++
            else -> nodes++
        }

        val fetchMore = nodes < limit || edges < limit
        documentCollector.truncated(!fetchMore)
        return fetchMore
    }


    override fun end() {

    }

    override fun getResult(): Any? {
        return null
    }
}
