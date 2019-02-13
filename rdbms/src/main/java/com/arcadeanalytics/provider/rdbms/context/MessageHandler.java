package com.arcadeanalytics.provider.rdbms.context;

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

import java.io.PrintStream;

/**
 * Implementation of PluginMessageHandler for Arcade Analytics plugin.
 * It receives messages application from the application and just delegates its printing on a stream through the OutputStreamManager.
 *
 * @author Gabriele Ponzi
 */
public class MessageHandler implements PluginMessageHandler {

    private int level;    // affects OutputStreamManager level
    private OutputStreamManager outputManager;

    public MessageHandler(PrintStream outputStream, int level) {
        this.level = level;
        this.outputManager = new OutputStreamManager(outputStream, level);
    }

    public MessageHandler(int level) {
        this.level = level;
        this.outputManager = new OutputStreamManager(level);
    }

    public MessageHandler(OutputStreamManager outputStreamManager) {
        this.outputManager = outputStreamManager;
        this.level = this.outputManager.getLevel();
    }

    public OutputStreamManager getOutputManager() {
        return this.outputManager;
    }

    public void setOutputManager(OutputStreamManager outputManager) {
        this.outputManager = outputManager;
    }


    @Override
    public int getOutputManagerLevel() {
        return this.level;
    }

    @Override
    public void setOutputManagerLevel(int level) {
        this.level = level;
        this.updateOutputStreamManagerLevel();
    }

    private synchronized void updateOutputStreamManagerLevel() {
        this.outputManager.setLevel(this.level);
    }

    @Override
    public synchronized void debug(Object requester, String message) {
        this.outputManager.debug(message);
    }

    @Override
    public synchronized void debug(Object requester, String format, Object... args) {
        this.outputManager.debug(format, args);
    }

    @Override
    public synchronized void info(Object requester, String message) {
        this.outputManager.info(message);
    }

    @Override
    public synchronized void info(Object requester, String format, Object... args) {
        this.outputManager.info(format, args);
    }

    @Override
    public synchronized void warn(Object requester, String message) {
        this.outputManager.warn(message);
    }

    @Override
    public synchronized void warn(Object requester, String format, Object... args) {
        this.outputManager.warn(format, args);
    }

    @Override
    public synchronized void error(Object requester, String message) {
        this.outputManager.error(message);
    }

    @Override
    public synchronized void error(Object requester, String format, Object... args) {
        this.outputManager.error(format, args);
    }
}
