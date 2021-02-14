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

data class DataSourceInfo(
    val id: Long,
    val type: String,
    val name: String,
    val description: String = "",
    val server: String,
    val port: Int,
    val database: String,
    val username: String,
    val password: String,
    val aggregationEnabled: Boolean = false,
    val connectionProperties: String = "{}",
    val enableSsl: Boolean = false,
    val remote: Boolean = false,
    val gateway: String = "",
    val sshPort: Int = 22,
    val sshUser: String = "",
    val skipCertValidation: Boolean = false
) {
    fun isAggregationEnabled(): Boolean = aggregationEnabled
}
