package com.arcadeanalytics.provider.rdbms.exception;

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

import java.io.IOException;

/**
 * It represents an IO Exception in Teleporter.
 *
 * @author Gabriele Ponzi
 */
@SuppressWarnings("serial")
public class RDBMSProviderIOException extends IOException {

  public RDBMSProviderIOException() {}

  public RDBMSProviderIOException(String message) {
    super(message);
  }

  public RDBMSProviderIOException(String message, Throwable cause) {
    super(message, cause);
  }

  public RDBMSProviderIOException(Throwable cause) {
    super(cause);
  }
}
