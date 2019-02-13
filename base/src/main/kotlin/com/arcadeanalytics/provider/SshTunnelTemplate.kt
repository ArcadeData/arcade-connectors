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

import com.google.common.collect.Sets
import com.google.common.io.ByteStreams
import com.jcraft.jsch.JSch
import com.jcraft.jsch.JSchException
import com.jcraft.jsch.Session
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.ServerSocket
import java.util.*
import java.util.function.BooleanSupplier
import java.util.function.Consumer

/**
 * Template class to be extended if an ssh tunnel should be created
 */
abstract class SshTunnelTemplate {

    protected fun buildTunnel(dataSourceInfo: DataSourceInfo, localPort: Int): Session {

        JSch.setLogger(JschSlf4jLogger())
        val jsch = JSch()

        try {
            val privKey = ByteStreams.toByteArray(javaClass.classLoader.getResourceAsStream("config/ssh/id_rsa"))
            val pubKey = ByteStreams.toByteArray(javaClass.classLoader.getResourceAsStream("config/ssh/id_rsa.pub"))
            jsch.addIdentity(Optional.ofNullable(dataSourceInfo.sshUser).orElse(DEFAULT_SSH_USER), privKey, pubKey, null)

            val session = jsch.getSession(dataSourceInfo.sshUser, dataSourceInfo.gateway, dataSourceInfo.sshPort)
            session.setConfig("StrictHostKeyChecking", "no")
            session.connect()

            session.setPortForwardingL(localPort, dataSourceInfo.server, dataSourceInfo.port)

            return session

        } catch (e: IOException) {
            throw RuntimeException(e)
        } catch (e: JSchException) {
            throw RuntimeException(e)
        }


    }

    protected fun findFreePort(): Int {
        var socket: ServerSocket? = null
        try {
            socket = ServerSocket(0)
            socket.reuseAddress = true
            val port = socket.localPort
            try {
                socket.close()
            } catch (e: IOException) {
                // Ignore IOException on close()
            }

            return port
        } catch (e: IOException) {
        } finally {
            if (socket != null) {
                try {
                    socket.close()
                } catch (e: IOException) {
                }

            }
        }
        throw IllegalStateException("Could not find a free TCP/IP port to open the ssh tunnel")
    }

    protected fun createLocalhostDataSource(dataSourceInfo: DataSourceInfo, localPort: Int): DataSourceInfo {

        return DataSourceInfo(dataSourceInfo.id,
                dataSourceInfo.type,
                dataSourceInfo.name,
                dataSourceInfo.description,
                "localhost",
                localPort,
                dataSourceInfo.database,
                dataSourceInfo.username,
                dataSourceInfo.password,
                dataSourceInfo.aggregationEnabled,
                dataSourceInfo.connectionProperties,
                false,
                "localhost",
                localPort,
                "")
    }

    fun supportedDataSourceTypes(): Set<String> {
        return Sets.newHashSet("SSH")
    }


    class JschSlf4jLogger : com.jcraft.jsch.Logger {
        private val logMap = HashMap<Int, Consumer<String>>()

        private val enabledMap = HashMap<Int, BooleanSupplier>()

        init {
            logMap[com.jcraft.jsch.Logger.DEBUG] = Consumer { log.debug(it) }
            logMap[com.jcraft.jsch.Logger.ERROR] = Consumer { log.error(it) }
            logMap[com.jcraft.jsch.Logger.FATAL] = Consumer { log.error(it) }
            logMap[com.jcraft.jsch.Logger.INFO] = Consumer { log.info(it) }
            logMap[com.jcraft.jsch.Logger.WARN] = Consumer { log.warn(it) }

            enabledMap[com.jcraft.jsch.Logger.DEBUG] = BooleanSupplier { log.isDebugEnabled }
            enabledMap[com.jcraft.jsch.Logger.ERROR] = BooleanSupplier { log.isErrorEnabled }
            enabledMap[com.jcraft.jsch.Logger.FATAL] = BooleanSupplier { log.isErrorEnabled }
            enabledMap[com.jcraft.jsch.Logger.INFO] = BooleanSupplier { log.isInfoEnabled }
            enabledMap[com.jcraft.jsch.Logger.WARN] = BooleanSupplier { log.isWarnEnabled }
        }

        override fun log(level: Int, message: String) {
            logMap[level]?.accept(message)
        }

        override fun isEnabled(level: Int): Boolean {
            return enabledMap[level]!!.getAsBoolean()
        }


    }

    companion object {

        private val log = LoggerFactory.getLogger(SshTunnelTemplate::class.java)

        private val DEFAULT_SSH_USER = "player"

        private val DEFAULT_SSH_PORT = 22
    }


}


