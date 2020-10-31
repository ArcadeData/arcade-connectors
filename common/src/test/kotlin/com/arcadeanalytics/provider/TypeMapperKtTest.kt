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

import org.assertj.core.api.Assertions
import org.junit.Test

class TypeMapperKtTest {

    @Test
    fun shouldMapNumericTypes() {
        listOf(
            "decimal", "dec", "numeric", "real", "integer",
            "int", "int2", "int4", "tinyint",
            "smallint unsigned", "tinyint unsigned", "mediumint unsigned",
            "float", "double precision",
            "smallint", "money", "double", "smallmoney"
        ).forEach {
            Assertions.assertThat(mapType(it)).isEqualTo("Numeric")
        }
    }

    @Test
    fun shouldMapTextTypes() {
        listOf(
            "string", "varchar", "text", "char",
            "varchar2", "nvarchar2", "clob", "nclob",
            "char varying", "character varying", "ntext", "nchar",
            "national char", "national character", "nvarchar", "national char varying",
            "national character varying", "longvarchar", "character large object",
            "mediumtext", "longtext", "tinytext"
        )
            .forEach {
                Assertions.assertThat(mapType(it)).isEqualTo("String")
            }
    }

    @Test
    fun shouldMapDateTypes() {
        listOf(
            "date", "datetime", "datetime2", "timestamp", "year",
            "smalldatetime", "datetimeoffset", "time with time zone"
        )
            .forEach {
                Assertions.assertThat(mapType(it)).isEqualTo("Date")
            }
    }

    @Test
    fun shouldMapBooleanTypes() {
        listOf("bool", "boolean")
            .forEach {
                Assertions.assertThat(mapType(it)).isEqualTo("Boolean")
            }
    }
}
