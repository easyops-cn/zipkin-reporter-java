/**
 * Copyright 2016 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.reporter;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ByteBoundedQueueTest {
  private final int MAX_SIZE = 10;
  private final int MAX_BYTES = 10;
  private final int MAX_TIMEOUT_NANOS = 1000000000;
  private final int MAX_MESSAGE_BYTES = 10;
  FakeSender sleepingSender = FakeSender.create().onSpans(spans -> {
    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  });
  private ReporterMetrics reporterMetrics = ReporterMetrics.NOOP_METRICS;
  private ByteBoundedQueue queue = new ByteBoundedQueue(
          MAX_SIZE, MAX_BYTES, MAX_MESSAGE_BYTES, MAX_TIMEOUT_NANOS,
          reporterMetrics, sleepingSender);

  @Test
  public void offer_failsWhenFull_size() {
    for (int i = 0; i < this.MAX_SIZE; i++) {
      assertThat(queue.offer(new byte[1])).isTrue();
    }
    assertThat(queue.offer(new byte[1])).isFalse();
  }

  @Test
  public void offer_failsWhenFull_sizeInBytes() {
    assertThat(queue.offer(new byte[this.MAX_BYTES])).isTrue();
    assertThat(queue.offer(new byte[1])).isFalse();
  }

  @Test
  public void offer_updatesCount() {
    for (int i = 0; i < this.MAX_SIZE; i++) {
      queue.offer(new byte[1]);
    }
    assertThat(queue.queuedSpans.get()).isEqualTo(this.MAX_SIZE);
  }

  @Test
  public void offer_sizeInBytes() {
    for (int i = 0; i < MAX_SIZE; i++) {
      queue.offer(new byte[1]);
    }
    assertThat(queue.sizeInBytes.get()).isEqualTo(MAX_SIZE);
  }
}
