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

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.util.Optional;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.platform.commons.util.AnnotationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ExternalResource<A extends Annotation, R extends AutoCloseable>
    implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalResource.class);

  private final Class<A> annotationClass;
  private final String key;
  private final ExtensionContext.Namespace namespace;

  protected ExternalResource(Class<A> annotationClass, String key, Namespace namespace) {
    this.annotationClass = annotationClass;
    this.key = key;
    this.namespace = namespace;
  }

  protected abstract A defaultSpec();

  protected abstract R createResource(A annotation, ExtensionContext context) throws Exception;

  protected R getResource(ExtensionContext context) {
    Store store = context.getStore(namespace);
    //noinspection unchecked
    return (R) store.get(key);
  }

  private void maybeStart(ExtensionContext context) throws Exception {
    Optional<AnnotatedElement> element = context.getElement();
    Optional<A> annotation = AnnotationUtils.findAnnotation(element, annotationClass);

    Store store = context.getStore(namespace);
    if (annotation.isPresent()) {
      A spec = annotation.get();
      LOG.info("Creating resource for {} in {}", spec, context.getUniqueId());
      AutoCloseable resource = createResource(spec, context);
      store.put(key, resource);
    } else {
      if (store.get(key) != null) {
        return;
      }

      // global/shared resource - synchronize creation
      synchronized (ExternalResource.class) {
        store = context.getRoot().getStore(namespace);
        if (store.get(key) != null) {
          return;
        }

        A spec = defaultSpec();
        LOG.info("Creating global resource for {} requested by {}", spec, context.getUniqueId());
        R resource = createResource(spec, context);
        store.put(key, resource);
      }
    }
  }

  private void maybeStop(ExtensionContext context) throws Exception {
    Optional<AnnotatedElement> element = context.getElement();
    Optional<A> annotation = AnnotationUtils.findAnnotation(element, annotationClass);

    if (annotation.isPresent()) {
      AutoCloseable resource = (AutoCloseable) context.getStore(namespace).get(key);
      if (resource != null) {
        resource.close();
      }
    }
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    maybeStart(context);
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    maybeStart(context);
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    maybeStop(context);
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    maybeStop(context);
  }
}
