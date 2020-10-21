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
package io.stargate.it;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

public class ExampleExt implements TestTemplateInvocationContextProvider, ExecutionCondition {

  public static final ConditionEvaluationResult DISABLED =
      ConditionEvaluationResult.disabled("Not running under example engine");
  public static final ConditionEvaluationResult ENABLE =
      ConditionEvaluationResult.enabled("Running under example engine");

  public ExampleExt() {}

  @Override
  public boolean supportsTestTemplate(ExtensionContext context) {
    return true;
  }

  private boolean enginePresent(ExtensionContext context) {
    return context.getUniqueId().contains("stargate-backend");
  }

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    if (enginePresent(context)) {
      return ENABLE;
    }

    return DISABLED;
  }

  @Override
  public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
      ExtensionContext context) {
    Set<String> tags = context.getTags();
    return Stream.of(new Ctx("x"), new Ctx("y"), new Ctx("z"));
  }

  private static class Ctx implements TestTemplateInvocationContext {
    private final String name;

    private Ctx(String name) {
      this.name = name;
    }

    @Override
    public String getDisplayName(int invocationIndex) {
      return name;
    }

    @Override
    public List<Extension> getAdditionalExtensions() {
      return Collections.emptyList();
    }
  }
}
