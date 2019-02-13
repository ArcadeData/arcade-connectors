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
package com.arcadeanalytics.provider.orient2

import com.orientechnologies.orient.core.record.impl.ODocument
import java.util.*

class OrientDBDocumentCollector {

    private val documents = ArrayList<ODocument>()
    internal var isTruncated = false

    fun collect(document: ODocument) {
        documents.add(document)
    }

    fun size(): Int {
        return documents.size
    }

    fun collected(): List<ODocument> {
        return documents
    }

    fun truncated(truncated: Boolean) {
        this.isTruncated = truncated
    }
}
