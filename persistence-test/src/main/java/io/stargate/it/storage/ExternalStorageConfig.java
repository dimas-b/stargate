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
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.extension.ExtensionContext;

public class ExternalStorageConfig {

  private static final Pattern BACKEND_TYPE_REGEX =
      Pattern.compile(".*\\[stargate-backend:([^]]+)].+");

  private final Properties properties;

  private ExternalStorageConfig(Properties properties) {
    this.properties = properties;
  }

  public static Optional<String> backend(ExtensionContext context) {
    Matcher matcher = BACKEND_TYPE_REGEX.matcher(context.getUniqueId());
    if (!matcher.matches()) {
      return Optional.empty();
    }

    String type = matcher.group(1);
    return Optional.of(type);
  }

  public static ExternalStorageConfig from(ExtensionContext context) {
    String backend =
        backend(context)
            .orElseThrow(() -> new IllegalStateException("Storage backend type not defined"));

    Properties properties = new Properties();
    try (InputStream is =
        ExternalStorageConfig.class
            .getClassLoader()
            .getResourceAsStream("stargate-backend-" + backend + ".properties")) {
      if (is == null) {
        throw new IllegalStateException("Storage backend properties not found for " + backend);
      }

      properties.load(is);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }

    return new ExternalStorageConfig(properties);
  }

  public ImmutableCcmWrapper.Builder configure(ImmutableCcmWrapper.Builder builder) {
    return builder
        .version(properties.getProperty("version"))
        .isDse(Boolean.parseBoolean(properties.getProperty("dse")));
  }
}
