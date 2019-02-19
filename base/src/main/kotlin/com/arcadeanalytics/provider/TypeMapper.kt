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
package com.arcadeanalytics.provider

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

val log: Logger = LoggerFactory.getLogger("com.arcadeanalytics.provider.TypeMapper")

fun mapType(type: String): String = when (type.trim()
        .toLowerCase(Locale.ENGLISH)
        .removePrefix("http://www.w3.org/2001/xmlschema#")) {
    "string", "varchar", "text", "char",
    "varchar2", "nvarchar2", "clob", "nclob",
    "char varying", "character varying", "ntext", "nchar",
    "national char", "national character", "nvarchar", "national char varying",
    "national character varying", "longvarchar", "character large object",
    "mediumtext", "longtext", "tinytext"
    -> "String"

    "decimal", "dec", "numeric", "real", "integer",
    "int", "int2", "int4", "tinyint",
    "smallint unsigned", "tinyint unsigned", "mediumint unsigned",
    "float", "double precision",
    "long", "smallint", "money", "smallmoney", "double"
    -> "Numeric"

    "date", "datetime", "datetime2", "timestamp", "year",
    "smalldatetime", "datetimeoffset", "time with time zone"
    -> "Date"

    "bool", "boolean"
    -> "Boolean"
    else -> {
        log.isDebugEnabled
        if (log.isDebugEnabled) log.debug("type not mapped:: {} ", type)
        type
    }

}
