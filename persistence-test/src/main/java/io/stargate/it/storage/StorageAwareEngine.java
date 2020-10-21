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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.engine.JupiterTestEngine;
import org.junit.jupiter.engine.config.JupiterConfiguration;
import org.junit.jupiter.engine.descriptor.ClassBasedTestDescriptor;
import org.junit.jupiter.engine.descriptor.JupiterEngineDescriptor;
import org.junit.jupiter.engine.execution.JupiterEngineExecutionContext;
import org.junit.platform.commons.util.AnnotationUtils;
import org.junit.platform.engine.EngineDiscoveryRequest;
import org.junit.platform.engine.ExecutionRequest;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestTag;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.support.config.PrefixedConfigurationParameters;
import org.junit.platform.engine.support.descriptor.AbstractTestDescriptor;
import org.junit.platform.engine.support.descriptor.EngineDescriptor;
import org.junit.platform.engine.support.hierarchical.ForkJoinPoolHierarchicalTestExecutorService;
import org.junit.platform.engine.support.hierarchical.HierarchicalTestEngine;
import org.junit.platform.engine.support.hierarchical.HierarchicalTestExecutorService;

/**
 * A JUnit 5 engine that runs tests several times, once for each pre-configured storage type.
 *
 * <p>This engine works in conjunction with the {@link ExternalStorage} extension.
 */
public class StorageAwareEngine extends HierarchicalTestEngine<JupiterEngineExecutionContext> {

  public static final String STARGATE_BACKEND_ID_SEGMENT_TYPE = "stargate-backend";
  public static final String ID = "stargate-engine";

  private final JupiterTestEngine delegate = new JupiterTestEngine();

  @Override
  public String getId() {
    return ID;
  }

  private List<String> backends() {
    try {
      List<String> backends = new ArrayList<>();
      Enumeration<URL> resources =
          getClass().getClassLoader().getResources("stargate-backends.txt");
      while (resources.hasMoreElements()) {
        URL url = resources.nextElement();
        try (BufferedReader lines = new BufferedReader(new InputStreamReader(url.openStream()))) {
          String line;
          while ((line = lines.readLine()) != null) {
            backends.add(line);
          }
        }
      }

      return backends;
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  protected HierarchicalTestExecutorService createExecutorService(ExecutionRequest request) {
    JupiterConfiguration config = getConfiguration(request);
    if (config.isParallelExecutionEnabled()) {
      return new ForkJoinPoolHierarchicalTestExecutorService(
          new PrefixedConfigurationParameters(
              request.getConfigurationParameters(), "junit.jupiter.execution.parallel.config."));
    }
    return super.createExecutorService(request);
  }

  private JupiterConfiguration getConfiguration(ExecutionRequest request) {
    RootDescriptor descriptor = (RootDescriptor) request.getRootTestDescriptor();
    return descriptor.configuration;
  }

  @Override
  protected JupiterEngineExecutionContext createExecutionContext(ExecutionRequest request) {
    JupiterConfiguration config = getConfiguration(request);
    return new JupiterEngineExecutionContext(request.getEngineExecutionListener(), config);
  }

  @Override
  public TestDescriptor discover(EngineDiscoveryRequest discoveryRequest, UniqueId rootId) {
    JupiterConfiguration configuration = null;
    List<TestDescriptor> discovered = new ArrayList<>();
    for (String backend : backends()) {
      UniqueId backendId = rootId.append(STARGATE_BACKEND_ID_SEGMENT_TYPE, backend);
      BackendDescriptor backendDescriptor = new BackendDescriptor(backendId, backend);

      UniqueId jupiterRoot = backendId.append("engine", delegate.getId());

      JupiterEngineDescriptor jupiterTests =
          (JupiterEngineDescriptor) delegate.discover(discoveryRequest, jupiterRoot);
      prune(new ArrayDeque<>(Collections.singleton(jupiterTests)));
      backendDescriptor.addChild(jupiterTests);

      configuration = jupiterTests.getConfiguration();
      discovered.add(backendDescriptor);
    }

    TestDescriptor root =
        new RootDescriptor(rootId, "Stargate Storage Integration Tests", configuration);
    discovered.forEach(root::addChild);
    return root;
  }

  private void prune(Deque<TestDescriptor> descriptors) {
    while (!descriptors.isEmpty()) {
      TestDescriptor desc = descriptors.poll();
      if (desc instanceof ClassBasedTestDescriptor) {
        if (!included((ClassBasedTestDescriptor) desc)) {
          desc.removeFromHierarchy();
          continue;
        }
      }

      descriptors.addAll(desc.getChildren());
    }
  }

  private boolean included(ClassBasedTestDescriptor desc) {
    Class<?> testClass = desc.getTestClass();
    return AnnotationUtils.findRepeatableAnnotations(testClass, ExtendWith.class).stream()
        .flatMap(e -> Arrays.stream(e.value()))
        .anyMatch(c -> c == ExternalStorage.class);
  }

  private static class RootDescriptor extends EngineDescriptor {
    private final JupiterConfiguration configuration;

    public RootDescriptor(UniqueId uniqueId, String displayName, JupiterConfiguration config) {
      super(uniqueId, displayName);
      this.configuration = config;
    }
  }

  private static class BackendDescriptor extends AbstractTestDescriptor {
    protected BackendDescriptor(UniqueId uniqueId, String backend) {
      super(uniqueId, backend);
    }

    @Override
    public Type getType() {
      return Type.CONTAINER;
    }

    @Override
    public Set<TestTag> getTags() {
      return Collections.singleton(TestTag.create("stargate-backend"));
    }
  }
}
