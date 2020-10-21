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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** JUnit 5 extension for tests that need a backend database cluster managed by {@code ccm}. */
public class ExternalStorage extends ExternalResource<ClusterSpec, ExternalStorage.Cluster>
    implements ParameterResolver, BeforeTestExecutionCallback, ExecutionCondition {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalStorage.class);

  private static final boolean EXTERNAL_BACKEND =
      Boolean.getBoolean("stargate.test.backend.use.external");
  private static final String DATACENTER = System.getProperty("stargate.test.backend.dc", "dc1");
  private static final String CLUSTER_NAME =
      System.getProperty("stargate.test.backend.cluster_name", "Test_Cluster");

  private static final ConditionEvaluationResult ENABLED =
      ConditionEvaluationResult.enabled("No reason to disable");

  private static final AtomicBoolean executing = new AtomicBoolean();

  private static final int clusterNodes = Integer.getInteger("stargate.test.backend.nodes", 1);

  public ExternalStorage() {
    super(ClusterSpec.class, "stargate-storage", Namespace.GLOBAL);
  }

  @Override
  protected Cluster createResource(ClusterSpec spec, ExtensionContext context) {
    String initSite = context.getDisplayName();
    ExternalStorageConfig storageConfig = ExternalStorageConfig.from(context);

    Cluster c = new Cluster(spec, storageConfig, initSite);
    c.start();
    return c;
  }

  @Override
  protected ClusterSpec defaultSpec() {
    return DefaultClusterSpecHolder.class.getAnnotation(ClusterSpec.class);
  }

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    if (!ExternalStorageConfig.backend(context).isPresent()) {
      return ConditionEvaluationResult.disabled(
          String.format(
              "Test %s can only be executed by %s", context.getUniqueId(), StorageAwareEngine.ID));
    }
    return ENABLED;
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return parameterContext.getParameter().getType().isAssignableFrom(ClusterConnectionInfo.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return getResource(extensionContext);
  }

  private String getFullTestPath(ExtensionContext context) {
    StringBuilder sb = new StringBuilder();
    while (context != null) {
      sb.insert(0, context.getDisplayName());
      sb.insert(0, '/');
      context = context.getParent().orElse(null);
    }
    return sb.toString();
  }

  @Override
  public void beforeTestExecution(ExtensionContext context) {
    LOG.info(
        "About to run {} with storage cluster version {}",
        getFullTestPath(context),
        getResource(context).clusterVersion());
  }

  @ClusterSpec
  private static class DefaultClusterSpecHolder {}

  protected static class Cluster implements ClusterConnectionInfo, AutoCloseable {

    private final UUID id = UUID.randomUUID();
    private final String initSite;
    private final CcmWrapper ccm;
    private final AtomicBoolean removed = new AtomicBoolean();

    private Cluster(ClusterSpec spec, ExternalStorageConfig config, String displayName) {
      this.initSite = displayName;
      this.ccm =
          config
              .configure(ImmutableCcmWrapper.builder())
              .clusterName(CLUSTER_NAME)
              .nodes(spec.nodes())
              .build();
    }

    public void start() {
      if (!EXTERNAL_BACKEND) {
        ccm.start();
        LOG.info("Storage cluster requested by {} has been started.", initSite);
      }
    }

    @Override
    public void close() throws Exception {
      stop();
    }

    public void stop() {
      if (!EXTERNAL_BACKEND) {
        ShutdownHook.remove(this);

        try {
          if (removed.compareAndSet(false, true)) {
            ccm.destroy();
            LOG.info(
                "Storage cluster (version {}) that was requested by {} has been removed.",
                clusterVersion(),
                initSite);
          }
        } catch (Exception e) {
          // This should not affect test result validity, hence logging as WARN
          LOG.warn("Exception during CCM cluster shutdown: {}", e.toString(), e);
        }
      }
    }

    @Override
    public String id() {
      return id.toString();
    }

    @Override
    public String seedAddress() {
      return ccm.seedAddress();
    }

    @Override
    public int storagePort() {
      return 7000;
    }

    @Override
    public int cqlPort() {
      return 9042;
    }

    @Override
    public String clusterName() {
      return CLUSTER_NAME;
    }

    @Override
    public String clusterVersion() {
      return ccm.version();
    }

    @Override
    public boolean isDse() {
      return ccm.isDse();
    }

    @Override
    public String datacenter() {
      return DATACENTER;
    }

    @Override
    public String rack() {
      return "rack1";
    }
  }
}
