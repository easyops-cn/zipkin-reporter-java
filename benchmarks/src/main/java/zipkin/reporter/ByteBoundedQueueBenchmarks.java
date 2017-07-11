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

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;

@Measurement(iterations = 5, time = 1)
@Warmup(iterations = 10, time = 1)
@Fork(3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Group)
public class ByteBoundedQueueBenchmarks {
  static final byte[] ONE = {1};

  @AuxCounters
  @State(Scope.Thread)
  public static class OfferCounters {
    public int offersFailed;
    public int offersMade;

    @Setup(Level.Iteration)
    public void clean() {
      offersFailed = offersMade = 0;
    }
  }

  @AuxCounters
  @State(Scope.Thread)
  public static class DrainCounters {
    public int drained;

    @Setup(Level.Iteration)
    public void clean() {
      drained = 0;
    }
  }

  private static ThreadLocal<Object> marker = new ThreadLocal<>();

  @State(Scope.Thread)
  public static class ConsumerMarker {
    public ConsumerMarker() {
      marker.set(this);
    }
  }

  ByteBoundedQueue q;
  @Param
  public Encoding encoding;
  static final InMemoryReporterMetrics metrics = new InMemoryReporterMetrics();

  @Setup
  public void setup() {
    q = new ByteBoundedQueue(10000, 10000, 10000, 1000000000,
            metrics, new NoopSender(encoding));
  }

  @Benchmark @Group("no_contention") @GroupThreads(1)
  public void no_contention_offer(OfferCounters counters) {
    if (q.offer(ONE)) {
      counters.offersMade++;
    } else {
      counters.offersFailed++;
    }
  }

  @Benchmark @Group("mild_contention") @GroupThreads(2)
  public void mild_contention_offer(OfferCounters counters) {
    if (q.offer(ONE)) {
      counters.offersMade++;
    } else {
      counters.offersFailed++;
    }
  }

  @Benchmark @Group("high_contention") @GroupThreads(8)
  public void high_contention_offer(OfferCounters counters) {
    if (q.offer(ONE)) {
      counters.offersMade++;
    } else {
      counters.offersFailed++;
    }
  }

  @TearDown(Level.Iteration)
  public void emptyQ() {
    // If this thread didn't drain, return
    if (marker.get() == null) return;
    q.clear();
  }
}
