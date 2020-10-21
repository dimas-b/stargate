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

import java.util.ArrayDeque;
import java.util.Deque;

public class ResourcePool {

  private static final Deque<Block> ipBlocks = new ArrayDeque<>();

  static {
    for (int i = 1; i < 50; i++) {
      ipBlocks.add(new Block(i));
    }
  }

  public static Block reserveIpBlock() {
    return ipBlocks.pollFirst();
  }

  public static void releaseIpBlock(Block block) {
    ipBlocks.addLast(block);
  }

  public static class Block {
    private final int num;

    private Block(int num) {
      this.num = num;
    }

    public String pattern() {
      // Assume at most 9 nodes per block
      return String.format("127.0.0.%02d%%d", num); // шюу. 127.0.0.NN%d
    }

    public String firstAddress() {
      return address(1);
    }

    public String address(int i) {
      return String.format(pattern(), i);
    }
  }
}
