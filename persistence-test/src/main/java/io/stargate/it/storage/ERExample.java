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

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;

public class ERExample extends ExternalResource<ClusterSpec, ERExample.Res> {

  protected ERExample() {
    super(ClusterSpec.class, "testKey", Namespace.GLOBAL);
  }

  @Override
  protected ClusterSpec defaultSpec() {
    System.out.println("AAA: defaultSpec");
    return null;
  }

  @Override
  protected Res createResource(ClusterSpec annotation, ExtensionContext context) {
    System.out.println("AAA: createResource: " + context.getUniqueId());
    return new Res(annotation);
  }

  static class Res implements AutoCloseable {
    private final ClusterSpec spec;

    private Res(ClusterSpec spec) {
      this.spec = spec;
    }

    @Override
    public void close() throws Exception {
      System.out.println("AAA: close: " + spec);
    }
  }
}
