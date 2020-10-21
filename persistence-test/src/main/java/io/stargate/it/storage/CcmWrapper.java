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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.FileUtils;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Immutable
public abstract class CcmWrapper implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(CcmWrapper.class);

  private static final long CCM_TIMEOUT =
      Long.getLong(
          "stargate.test.ccm.exec.timeout.millis",
          600_000); // 10 min (mostly to avoid timeouts in CI)
  private static final AtomicInteger cmdSequence = new AtomicInteger();

  private final Path configDir;
  private final ResourcePool.Block ipBlock = ResourcePool.reserveIpBlock();

  public CcmWrapper() {
    try {
      this.configDir = Files.createTempDirectory("ccm");
      this.configDir.toFile().deleteOnExit();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public abstract String version();

  public abstract boolean isDse();

  public abstract String clusterName();

  public abstract int nodes();

  public String seedAddress() {
    return ipBlock.firstAddress();
  }

  private CommandLine ccm(String command) {
    return new CommandLine("ccm").addArgument(command);
  }

  public final void start() {
    CommandLine create = ccm("create");
    create.addArgument(clusterName());

    create.addArgument("-I");
    create.addArgument(ipBlock.pattern());

    create.addArgument("-n");
    create.addArgument("" + nodes() + ":0");

    create.addArgument("-v");
    create.addArgument(version());

    if (isDse()) {
      create.addArgument("--dse");
    }

    exec(create);

    CommandLine start = ccm("start");
    create.addArgument("--wait-for-binary-proto");

    exec(start);

    ShutdownHook.add(this);
  }

  public final void destroy() {
    ShutdownHook.remove(this);

    exec(ccm("remove"));

    ResourcePool.releaseIpBlock(ipBlock);

    try {
      FileUtils.deleteDirectory(configDir.toFile());
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void close() throws Exception {
    destroy();
  }

  private void exec(CommandLine cmd) {
    int seqNum = cmdSequence.incrementAndGet();
    String logPrefix = "ccm" + seqNum;
    OutputStreamLogger out = new OutputStreamLogger(logPrefix, false);
    OutputStreamLogger err = new OutputStreamLogger(logPrefix, true);
    Executor executor = new DefaultExecutor();
    executor.setStreamHandler(new PumpStreamHandler(out, err));
    ExecuteWatchdog watchDog = new ExecuteWatchdog(CCM_TIMEOUT);
    executor.setWatchdog(watchDog);

    cmd.addArgument("--config-dir");
    cmd.addArgument(configDir.toFile().getAbsolutePath());

    try {
      LOG.info("Running command #{}: {}", seqNum, cmd);

      int retValue = executor.execute(cmd);

      LOG.info("Command #{} returned with exit value: {}", seqNum, retValue);
    } catch (Exception e) {
      LOG.info("Error in command #{}: {}", seqNum, e.getMessage(), e);
      throw new IllegalStateException(e);
    }
  }
}
