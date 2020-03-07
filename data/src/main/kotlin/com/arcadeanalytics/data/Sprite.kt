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

import com.google.common.collect.ArrayListMultimap
import com.google.common.collect.ListMultimap
import com.google.common.collect.Lists
import java.util.*
import java.util.regex.Pattern

/**
 * The Sprite is a flexible data container based on ListMultimap to be used in ETL tasks
 *
 * The unit test ha a lot of usage examples
 *
 * @author Roberto Franchini
 */
private const val copySuffix = "____COPY___"

class Sprite {

    val data: ListMultimap<String, Any?> = ArrayListMultimap.create()

    fun add(field: String, value: Any?): Sprite {
        if (value is Collection<*>)
            value.asSequence()
                    .filter { it != null }
                    .forEach { v -> data.put(field, v) }
        else if (value != null) data.put(field, value)
        return this
    }

    fun add(field: String, value: String): Sprite {
        if (value.isNotBlank())
            data.put(field, value)
        return this
    }


    fun entries(): MutableCollection<MutableMap.MutableEntry<String, Any?>>? {
        return data.entries()
    }

    fun fields(): Set<String> {
        return HashSet(data.keySet())
    }

    fun fields(pattern: Pattern): Set<String> {
        return data.keySet()
                .filter { k -> pattern.matcher(k).matches() }
                .toSet()
    }

    fun fields(pattern: Regex): Set<String> {
        return data.keySet()
                .filter { k -> pattern.matches(k) }
                .toSet()
    }

    fun rename(field: String, renamed: String): Sprite {
        with(data) {
            copy(field, renamed)
            removeAll(field)
        }
        return this
    }


    fun copy(from: String, to: String): Sprite {
        with(data) {
            val fromValues = Lists.newArrayList(get(from))
            addAll(to, fromValues)

        }
        return this
    }


    fun addAll(field: String, values: Iterable<Any?>): Sprite {
        values.forEach { add(field, it) }
        return this
    }

    fun addAll(field: String, values: List<Any?>): Sprite {
        values.forEach { add(field, it) }
        return this
    }

    fun addAllIfNotExists(field: String, values: Iterable<Any>): Sprite {
        values.forEach { addIfNotExists(field, it) }
        return this
    }

    fun addIfNotExists(field: String, fieldValue: Any): Sprite {

        if (hasNotValue(field, fieldValue)) {
            add(field, fieldValue)
        }

        return this
    }

    fun hasNotValue(value: Any): Boolean {
        return !data.containsValue(value)
    }

    fun hasNotValue(field: String, value: Any): Boolean {
        return !data.containsEntry(field, value)
    }

    fun hasValue(value: Any): Boolean {
        return data.containsValue(value)
    }

    fun hasValue(field: String, value: Any): Boolean {
        return data.containsEntry(field, value)
    }

    fun hasField(field: String): Boolean {
        return data.containsKey(field)
    }

    fun load(input: Map<String, Any>): Sprite {
        input.entries
                .forEach { add(it.key, it.value) }
        return this
    }


    fun remove(field: String, fieldValue: Any): Sprite {
        data.remove(field, fieldValue)
        return this
    }

    fun remove(field: String): Sprite {
        data.removeAll(field)
        return this
    }

    fun remove(pattern: Pattern): Sprite {
        val fields = fields(pattern)
        return remove(fields)
    }

    fun remove(fields: Collection<String>): Sprite {
        fields.forEach { f -> data.removeAll(f) }
        return this
    }


    fun <F : Any, T : Any> apply(pattern: Pattern, fieldModifier: (F) -> T): Sprite {
        fields(pattern)
                .forEach { f -> apply(f, fieldModifier) }

        return this

    }

    fun <F : Any, T : Any> apply(pattern: Regex, fieldModifier: (F) -> T): Sprite {
        fields(pattern)
                .forEach { f -> apply(f, fieldModifier) }

        return this

    }

    fun <F : Any, T : Any> apply(field: String, fieldModifier: (F) -> T): Sprite {

        if (hasField(field)) {
            val newValues = newValuesOf(field, fieldModifier)
            remove(field)
            addAll(field, newValues)
        }

        return this
    }

    fun <F : Any, T : Any> apply(from: String, transformer: (F) -> T, to: String): Sprite {

        if (hasField(from)) {
            val newValues = newValuesOf(from, transformer)
            addAll(to, newValues)
        }

        return this
    }

    private fun <F : Any, T : Any> newValuesOf(field: String, fieldModifier: (F) -> T): List<Any?> {
        val originalValues: List<F> = rawValuesOf(field)
        return originalValues.map { it -> fieldModifier(it) }.toList()
    }


    fun <T : Any> rawValuesOf(field: String): List<T> {
        return data.get(field).orEmpty() as List<T>
    }


    fun rename(field: Pattern, renamed: (v: String) -> String): Sprite {
        fields(field)
                .forEach { f ->
                    val cleaned = renamed(f)
                    rename(f, cleaned)
                }

        return this
    }

    fun joinValuesOf(field: String,
                     separator: CharSequence = ", ",
                     prefix: CharSequence = "",
                     postfix: CharSequence = "",
                     limit: Int = -1,
                     truncated: CharSequence = "..."): Sprite {

        val merged = rawValuesOf<String>(field)
                .joinToString(separator, prefix, postfix, limit, truncated)

        remove(field)
                .add(field, merged)
        return this

    }


    fun valueOf(field: String): String {
        return rawValueOf<Any>(field).let(Any::toString)
    }

    fun valuesOf(field: String): List<String> {

        return rawValuesOf<Any>(field).map { it -> it.toString() }.toList()
    }

    fun valuesOf(regex: Regex): List<String> {

        return fields(regex)
                .map { field ->
                    rawValuesOf<Any>(field)
                            .map { it.toString() }
                }
                .flatMap {
                    it.toList()
                }
                .toList()

    }

    fun valuesOf(regex: Pattern): List<String> {

        return fields(regex)
                .map { field ->
                    rawValuesOf<Any>(field)
                            .map { it.toString() }
                }
                .flatMap {
                    it.toList()
                }
                .toList()

    }


    fun <T : Any> rawValueOf(field: String): T {
        return rawValuesOf<T>(field).first()
    }


    fun isMultiValue(field: String): Boolean {
        return data.get(field).size > 1
    }

    fun isSingleValue(field: String): Boolean {
        return data.get(field).size == 1
    }

    fun sizeOf(field: String): Int {
        return data.get(field).size
    }

    fun asMultimap(): MutableMap<String, MutableCollection<Any?>>? {
        return data.asMap()
    }

    fun asMap(): MutableMap<String, Any?>? {
        val map = HashMap<String, Any?>()
        for (key in data.keySet()) {
            val values = data.get(key)
            map[key] = values.iterator().next()
        }

        return map
    }


    fun asStringMap(): Map<String, String> {
        val map = HashMap<String, String>()

        fields().map { f -> map.put(f, valueOf(f)) }

        return map
    }


    fun splitValues(field: String, separator: String): Sprite {

        val copySuffix = copySuffix
        copy(field, "$field$copySuffix")
                .remove(field)
                .valuesOf("$field$copySuffix")
                .map { value -> value.split(separator) }
                .forEach { splitted -> add(field, splitted) }

        apply(field, { v: String -> v.trim() })

        remove("$field$copySuffix")

        return this
    }

    fun isEmpty(): Boolean {

        return data.isEmpty
    }


    override fun hashCode(): Int {
        val prime = 31
        var result = 1
        result = prime * result + data.hashCode()
        return result
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null) return false
        if (javaClass != other.javaClass) return false

        val otherSprite = other as Sprite

        if (data != otherSprite.data) return false

        return true
    }

    override fun toString(): String {

        return data.toString()
    }
}
