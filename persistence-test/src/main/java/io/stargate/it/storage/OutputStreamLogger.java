/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.it.storage;

import java.util.concurrent.CountDownLatch;
import org.apache.commons.exec.LogOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutputStreamLogger extends LogOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(OutputStreamLogger.class);

  private final String prefix;
  private final boolean error;
  private final CountDownLatch latch;
  private final String tag;

  public OutputStreamLogger(String prefix, boolean error) {
    this(prefix, error, null, null);
  }

  public OutputStreamLogger(String prefix, boolean error, CountDownLatch latch, String tag) {
    this.prefix = prefix;
    this.error = error;
    this.latch = latch;
    this.tag = tag;
  }

  @Override
  protected void processLine(String line, int logLevel) {
    if (error) {
      LOG.error("{}> {}", prefix, line);
    } else {
      LOG.info("{}> {}", prefix, line);
    }

    if (tag != null && line.contains(tag)) {
      latch.countDown();
    }
  }
}
