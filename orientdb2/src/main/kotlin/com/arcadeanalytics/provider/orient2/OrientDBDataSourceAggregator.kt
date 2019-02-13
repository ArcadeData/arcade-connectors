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

import com.arcadeanalytics.provider.DataSourceInfo
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
import org.slf4j.LoggerFactory

class OrientDBDataSourceAggregator {
    private val log = LoggerFactory.getLogger(OrientDBDataSourceAggregator::class.java)


    fun aggregate(dataSource: DataSourceInfo,
                  classes: Set<String>,
                  fields: Set<String>,
                  minDocCount: Long,
                  maxValuesPerField: Long) {


        log.info("fetching data from '{}' aggregating on '{}/{}' ", dataSource.id, classes, fields)


        open(dataSource).use {


            val className = classes.first()

            val fieldName = fields.first()

//            val query = """SELECT $fieldName,
//                                    count($fieldName) AS doc_count
//                                    FROM $className
//                                    GROUP BY $fieldName   
//                                    ORDER BY doc_count DESC
//                                    LIMIT $maxValuesPerField
//                                """.trimIndent()

            val query = """MATCH
                                {class:Person, where: (age=45)} -FriendOf-> {class:Person}

                                """.trimIndent()




            log.info("query:: {} ", query)

            val docs = it.query<List<ODocument>>(OSQLSynchQuery<ODocument>(query))



            docs.asSequence()
                    .forEach { d -> log.info("{}={}", d.field("age"), d.field("doc_count")) }
        }

    }

}
