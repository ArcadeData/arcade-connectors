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
package com.arcadeanalytics.data

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.util.Lists
import org.assertj.guava.api.Assertions.assertThat
import org.assertj.guava.api.Assertions.entry
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.regex.Pattern

class SpriteShould {

    private lateinit var sprite: Sprite

    @BeforeEach
    internal fun setUp() {
        sprite = Sprite()
    }

    @Test
    internal fun `add values of different types`() {
        sprite.add("field1", "value1")
                .add("field2", 10)
                .add("field3", false)

        assertThat(sprite.entries()).contains(
                entry("field1", "value1"),
                entry("field2", 10),
                entry("field3", false)
        )

    }

    @Test
    internal fun `flat collection value`() {

        val values = mutableListOf<String>("value1", "value2", "value3")

        sprite.add("field1", values)
                .add("field2", 10)
                .add("field3", false)

        assertThat(sprite.rawValuesOf<String>("field1")).isNotEmpty
                .hasSize(3)

        assertThat(sprite.entries()).contains(
                entry("field1", "value1"),
                entry("field1", "value2"),
                entry("field1", "value3"),
                entry("field2", 10),
                entry("field3", false)
        )


    }

    @Test
    internal fun `return single value as string`() {
        sprite.add("field1", "value1")
                .add("field2", 10)
                .add("field3", false)

        assertThat(sprite.valueOf("field1")).isEqualTo("value1")
        assertThat(sprite.valueOf("field2")).isEqualTo("10")
        assertThat(sprite.valueOf("field3")).isEqualTo("false")

    }

    @Test
    internal fun `return multi value as string`() {
        sprite.add("field1", 10)
                .add("field1", 20)
                .add("field1", 30)


        assertThat(sprite.valuesOf("field1")).contains("10", "20", "30")

    }

    @Test
    internal fun `return multi value as int type`() {
        sprite.add("field1", 10)
                .add("field1", 20)
                .add("field1", 30)


        assertThat(sprite.rawValuesOf<Int>("field1")).contains(10, 20, 30)

    }

    @Test
    internal fun `return single value typed`() {
        sprite.add("field1", "value1")
                .add("field2", 10)
                .add("field3", false)

        assertThat(sprite.rawValueOf<String>("field1")).isEqualTo("value1")
        assertThat(sprite.rawValueOf<Int>("field2")).isEqualTo(10)
        assertThat(sprite.rawValueOf<Boolean>("field3")).isEqualTo(false)

    }

    @Test
    internal fun `copy single value field`() {
        sprite.add("field", "value")
                .copy("field", "copyOfField")

        assertThat(sprite.entries()).contains(
                entry("field", "value"),
                entry("copyOfField", "value")

        )
    }

    @Test
    internal fun `copy multi values field`() {
        sprite.add("field", "value1")
                .add("field", "value2")
                .add("field", "value3")
                .add("field", "value4")
                .copy("field", "copyOfField")

        assertThat(sprite.entries()).isNotEmpty

        assertThat(sprite.entries()).contains(
                entry("field", "value1"),
                entry("field", "value2"),
                entry("field", "value3"),
                entry("field", "value4"),
                entry("copyOfField", "value1"),
                entry("copyOfField", "value2"),
                entry("copyOfField", "value3"),
                entry("copyOfField", "value4")
        )

    }

    @Test
    internal fun `not adds empty values`() {
        sprite.addAll("field", Lists.emptyList())

        assertThat(sprite.data).isEmpty()

        sprite.add("field", "")

        assertThat(sprite.data).isEmpty()

    }


    @Test
    internal fun `rename field`() {
        sprite.add("field", "value1")
                .add("field", "value2")
                .add("field", "value3")
                .add("field", "value4")
                .rename("field", "renamed")

        assertThat(sprite.data).contains(
                entry("renamed", "value1"),
                entry("renamed", "value2"),
                entry("renamed", "value3"),
                entry("renamed", "value4")
        )
    }


    @Test
    internal fun `rename with lambda`() {
        sprite.add("a_field", "value1")
                .rename(Pattern.compile("a_.*")) { v: String -> v.removePrefix("a_") }

        assertThat(sprite.data)
                .contains(entry("field", "value1"))
                .hasSize(1)


    }

    @Test
    internal fun `join field values`() {
        sprite.add("field", "value1")
                .add("field", "value2")
                .add("field", "value3")
                .add("field", "value4")
                .joinValuesOf("field", "\n", "VALUES\n", "\nEND")

        assertThat(sprite.isSingleValue("field")).isTrue()
        assertThat(sprite.valueOf("field"))
                .isEqualTo("""VALUES
                    |value1
                    |value2
                    |value3
                    |value4
                    |END""".trimMargin())

    }

    @Test
    internal fun `split field value`() {
        sprite.add("field", "value1 value2 value3 value4")
                .splitValues("field", " ")

        assertThat(sprite.isMultiValue("field")).isTrue()
        assertThat(sprite.valuesOf("field"))
                .contains("value1", "value2", "value3", "value4")
    }


    @Test
    internal fun `be idempotent on renaming not present field`() {
        sprite.rename("notPresent", "renamed")

        assertThat(sprite.isEmpty()).isTrue()

    }

    @Test
    internal fun `retrieve field names`() {
        sprite.add("field", "value")
                .add("field2", "value2")
                .add("field3", "value3")

        assertThat(sprite.fields())
                .contains("field", "field2", "field3")

    }

    @Test
    internal fun `retrieve field names with regexp`() {
        sprite.add("field1", "value")
                .add("field2", "value2")
                .add("field3", "value3")
                .add("a_field", "value")

        assertThat(sprite.fields(Pattern.compile("field.*")))
                .contains("field1", "field2", "field3")

        assertThat(sprite.fields(Regex("field.*")))
                .contains("field1", "field2", "field3")

        assertThat(sprite.fields(Regex("a_.*")))
                .contains("a_field")

    }

    @Test
    internal fun `retrieve field string values with regexp`() {
        sprite.add("field1", "value")
                .add("field2", "value2")
                .add("field3", "value3")
                .add("a_field", "value")

        assertThat(sprite.valuesOf(Pattern.compile("field.*")))
                .contains("value", "value2", "value3")

        assertThat(sprite.valuesOf(Regex("field.*")))
                .contains("value", "value2", "value3")

        assertThat(sprite.valuesOf(Regex("a_.*")))
                .contains("value")

    }

    @Test
    internal fun `say if value exists`() {
        sprite.add("field", "value")

        assertThat(sprite.hasValue("value")).isTrue()
        assertThat(sprite.hasValue("field", "value")).isTrue()

        assertThat(sprite.hasNotValue("notPresent")).isTrue()
        assertThat(sprite.hasNotValue("field", "NotPresent")).isTrue()

    }


    @Test
    internal fun `remove field`() {
        sprite.add("field", "value")

        sprite.remove("field")

        assertThat(sprite.data).isEmpty()
    }

    @Test
    internal fun `remove fields`() {
        sprite.add("field", "value")
                .add("field", "value2")
                .add("field2", "value2")

        sprite.remove(listOf("field", "field2"))

        assertThat(sprite.data).isEmpty()
    }

    @Test
    internal fun `remove fields with pattern`() {
        sprite.add("field", "value")
                .add("field", "value2")
                .add("field2", "value2")

        sprite.remove(Pattern.compile("field.*"))

        assertThat(sprite.data).isEmpty()
    }

    @Test
    internal fun `load from map`() {
        val now = LocalDate.now()
        val input = mapOf(
                "field1" to "value1",
                "field2" to "value2",
                "field3" to now)

        sprite.load(input)

        assertThat(sprite.hasField("field1")).isTrue()
        assertThat(sprite.hasField("field2")).isTrue()

        assertThat(sprite.hasField("field3")).isTrue()
        assertThat(sprite.valueOf("field3")).isEqualTo(now.toString())


    }

    @Test
    internal fun `apply lambda to all field values`() {
        sprite.add("field", "value")
                .add("field", "value2")

        sprite.apply("field", String::toUpperCase)

        assertThat(sprite.data).isNotEmpty()

        assertThat(sprite.valuesOf("field")).contains("VALUE", "VALUE2")
    }

    @Test
    internal fun `apply lambda to all fields matching a pattern`() {
        sprite.add("firstField", "value")
                .add("secondField", "value2")

        sprite.apply(Pattern.compile(".*Field"), String::toUpperCase)

        assertThat(sprite.data).isNotEmpty()

        assertThat(sprite.valuesOf("firstField")).contains("VALUE")
        assertThat(sprite.valuesOf("secondField")).contains("VALUE2")
    }

    @Test
    internal fun `apply lambda to all field values and store on another field`() {
        sprite.add("field", "value")
                .add("field", "value2")

        sprite.apply("field", String::toUpperCase, "to")


        assertThat(sprite.data).isNotEmpty()

        assertThat(sprite.valuesOf("field")).contains("value", "value2")
        assertThat(sprite.valuesOf("to")).contains("VALUE", "VALUE2")
    }


    @Test
    internal fun `return map with single value`() {
        val now = LocalDate.now()
        val map = sprite.add("field", "value")
                .add("field", "value2")
                .add("field", "value3")
                .add("dateField", now)
                .asMap()

        assertThat(map?.get("field")).isEqualTo("value")
        assertThat(map?.get("dateField")).isEqualTo(now)
    }

    @Test
    internal fun `return map with single value as string`() {
        val now = LocalDate.now()
        val map = sprite.add("field", "value")
                .add("field", "value2")
                .add("field", "value3")
                .add("dateField", now)
                .asStringMap()

        assertThat(map.get("field")).isEqualTo("value")


        assertThat(map.get("dateField")).isEqualTo(now.toString())


    }

    @Test
    internal fun `transform sprite to mutable map of collections`() {
        val map = sprite.add("field", "value")
                .add("field", "value2")
                .add("field", "value3")
                .add("single", "singleValue")
                .asMultimap()

        assertThat(map?.get("field"))
                .hasSize(3)
                .contains("value", "value2", "value3")

        assertThat(map?.get("single"))
                .hasSize(1)
                .containsOnly("singleValue")

    }


    @Test
    internal fun `answer about field's size`() {

        sprite.add("field", "value")
                .add("field", "value2")
                .add("field", "value3")
                .add("dateField", LocalDate.now())

        //field
        assertThat(sprite.isMultiValue("field")).isTrue()
        assertThat(sprite.isSingleValue("field")).isFalse()
        assertThat(sprite.sizeOf("field")).isEqualTo(3)


        //dateField
        assertThat(sprite.isMultiValue("dateField")).isFalse()
        assertThat(sprite.isSingleValue("dateField")).isTrue()
        assertThat(sprite.sizeOf("dateField")).isEqualTo(1)

    }

    @Test
    internal fun `consider two sprites equals`() {
        sprite.add("f", "v")

        val other = Sprite().add("f", "v")

        assertThat(sprite).isEqualTo(other)
    }

    @Test
    internal fun `view date as string`() {
        sprite.add("text", "the text")
                .add("text", "another text")
                .add("date", LocalDate.parse("20180901", DateTimeFormatter.ofPattern("yyyyMMdd")))
                .add("age", 10)


        assertThat(sprite.toString())
                .isEqualTo("{date=[2018-09-01], text=[the text, another text], age=[10]}")
    }




    @Test
    internal fun `show an ETL example`() {

        val data = Sprite()
                .add("age", 90)
                .add("name", "rob")
                .add("text", "first phrase")
                .add("text", "second phrase")
                .add("text", "third phrase")
                .add("text", "fourth phrase")
                .rename("age", "weight")
                .apply("weight") { v: Float -> v * 2.2 }
                .apply("name", String::toUpperCase)
                .joinValuesOf("text")

        assertThat(data.hasField("age"))
                .isFalse()
        assertThat(data.rawValueOf<Float>("weight"))
                .isEqualTo(198.0f)
        assertThat(data.valueOf("name"))
                .isEqualTo("ROB")
        assertThat(data.valueOf("text"))
                .isEqualTo("first phrase, second phrase, third phrase, fourth phrase")
    }


}
