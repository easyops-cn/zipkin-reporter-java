/**
 * Copyright 2016-2017 The OpenZipkin Authors
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

import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Test;
import zipkin.Annotation;
import zipkin.Span;
import zipkin.TestObjects;
import zipkin.reporter.AsyncReporter.BoundedAsyncReporter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class AsyncReporterTest {

  Span span = TestObjects.TRACE.get(2);
  int sizeInBytesOfSingleSpanMessage =
      Encoding.THRIFT.listSizeInBytes(Collections.singletonList(Encoder.THRIFT.encode(span)));

  AsyncReporter<Span> reporter;
  InMemoryReporterMetrics metrics = new InMemoryReporterMetrics();

  @After
  public void close() {
    if (reporter != null) reporter.close();
  }

  @Test
  public void messageMaxBytes_defaultsToSender() throws InterruptedException {
    AtomicInteger sentSpans = new AtomicInteger();
    reporter = AsyncReporter.builder(FakeSender.create()
        .onSpans(spans -> sentSpans.addAndGet(spans.size()))
        .messageMaxBytes(sizeInBytesOfSingleSpanMessage))
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build();

    reporter.report(span);
    Thread.sleep(20);  // schedule consumer thread.
    reporter.flush();

    assertThat(sentSpans.get()).isEqualTo(1);
  }

  @Test
  public void messageMaxBytes_queuesWhenOverMessageMaxBytes() throws InterruptedException {
    AtomicInteger sentSpans = new AtomicInteger();
    reporter = AsyncReporter.builder(FakeSender.create()
        .onSpans(spans -> sentSpans.addAndGet(spans.size())))
        .messageMaxBytes(sizeInBytesOfSingleSpanMessage)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .metrics(metrics)
        .build();

    reporter.report(span);
    reporter.report(span);
    Thread.sleep(20);  // schedule consumer thread

    assertThat(sentSpans.get()).isEqualTo(1);
    assertThat(metrics.queuedSpans()).isEqualTo(1);
  }

  @Test
  public void messageMaxBytes_dropsWhenTooLarge() throws InterruptedException {
    AtomicInteger sentSpans = new AtomicInteger();
    reporter = AsyncReporter.builder(FakeSender.create()
        .onSpans(spans -> sentSpans.addAndGet(spans.size())))
        .messageMaxBytes(sizeInBytesOfSingleSpanMessage)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build();

    reporter.report(span.toBuilder().addAnnotation(Annotation.create(1L, "fooooo", null)).build());
    Thread.sleep(20);  // schedule consumer thread
    reporter.flush();

    assertThat(sentSpans.get()).isEqualTo(0);
  }

  @Test
  public void queuedMaxSpans_dropsWhenOverqueuing() throws InterruptedException {
    AtomicInteger sentSpans = new AtomicInteger();
    reporter = AsyncReporter.builder(FakeSender.create()
        .onSpans(spans -> sentSpans.addAndGet(spans.size())))
        .queuedMaxSpans(1)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build();

    reporter.report(span);
    reporter.report(span); // dropped the one that queued more than allowed count
    Thread.sleep(20);  // schedule consumer thread
    reporter.flush();
    assertThat(sentSpans.get()).isEqualTo(1);
  }

  @Test
  public void report_incrementsMetrics() {
    reporter = AsyncReporter.builder(FakeSender.create())
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build();

    reporter.report(span);
    reporter.report(span);
    assertThat(metrics.spans()).isEqualTo(2);
    assertThat(metrics.spanBytes()).isEqualTo(Encoder.THRIFT.encode(span).length * 2);
  }

  @Test
  public void report_incrementsSpansDropped() {
    reporter = AsyncReporter.builder(FakeSender.create())
        .queuedMaxSpans(1)
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build();

    reporter.report(span);
    reporter.report(span);

    assertThat(metrics.spans()).isEqualTo(2);
    assertThat(metrics.spansDropped()).isEqualTo(1);
  }

  @Test
  public void flush_incrementsMetrics() throws InterruptedException {
    reporter = AsyncReporter.builder(FakeSender.create())
        .metrics(metrics)
        .messageMaxBytes(sizeInBytesOfSingleSpanMessage)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build();

    // Queue up 2 spans
    reporter.report(span);
    reporter.report(span);
    Thread.sleep(20);  // schedule consumer thread

    assertThat(metrics.queuedSpans()).isEqualTo(1); // still one span in the backlog
    assertThat(metrics.queuedBytes()).isEqualTo(Encoder.THRIFT.encode(span).length);
    assertThat(metrics.messages()).isEqualTo(1);
    assertThat(metrics.messageBytes()).isEqualTo(sizeInBytesOfSingleSpanMessage);

    reporter.flush();
    assertThat(metrics.queuedSpans()).isZero();
    assertThat(metrics.queuedBytes()).isZero();
    assertThat(metrics.messages()).isEqualTo(2);
    assertThat(metrics.messageBytes()).isEqualTo(sizeInBytesOfSingleSpanMessage * 2);
  }

  @Test
  public void flush_incrementsMessagesDropped() throws InterruptedException {
    reporter = AsyncReporter.builder(FakeSender.create()
        .onSpans(spans -> {
          throw new RuntimeException();
        }))
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build();

    reporter.report(span);
    Thread.sleep(20);  // schedule consumer thread

    reporter.flush();
    assertThat(metrics.messagesDropped()).isEqualTo(1);
  }

  /** It can take up to the messageTimeout past the first span to send */
  @Test
  public void messageTimeout_flushesWhenTimeoutExceeded() throws InterruptedException {
    CountDownLatch sentSpans = new CountDownLatch(1);
    reporter = AsyncReporter.builder(FakeSender.create()
        .onSpans(spans -> sentSpans.countDown()))
        .messageTimeout(10, TimeUnit.MILLISECONDS)
        .build();

    reporter.report(span);
    assertThat(sentSpans.await(5, TimeUnit.MILLISECONDS))
        .isFalse();
    assertThat(sentSpans.await(10, TimeUnit.MILLISECONDS))
        .isTrue();
  }

  @Test
  public void messageTimeout_disabled() throws InterruptedException {
    CountDownLatch sentSpans = new CountDownLatch(1);
    reporter = AsyncReporter.builder(FakeSender.create()
        .onSpans(spans -> sentSpans.countDown()))
        .messageTimeout(0, TimeUnit.NANOSECONDS)
        .build();

    reporter.report(span);
    assertThat(sentSpans.getCount()).isEqualTo(1);

    // Since no threads started, the above lingers
    assertThat(sentSpans.await(10, TimeUnit.MILLISECONDS))
        .isFalse();
  }

  @Test
  public void senderThread_threadHasAPrettyName() throws InterruptedException {
    BlockingQueue<String> threadName = new LinkedBlockingQueue<>();
    reporter = AsyncReporter.builder(FakeSender.create()
        .onSpans(spans -> threadName.offer(Thread.currentThread().getName())))
        .build();

    reporter.report(span);

    // check name is pretty
    assertThat(threadName.take())
        .isEqualTo("AsyncReporter(FakeSender)");
  }

  @Test
  public void close_close_stopsSender() throws InterruptedException {
    reporter = AsyncReporter.builder(FakeSender.create())
        .metrics(metrics)
        .messageTimeout(2, TimeUnit.MILLISECONDS)
        .build();

    reporter.close();

    // the close latch counts down when the thread is stopped
    BoundedAsyncReporter<Span> impl = (BoundedAsyncReporter<Span>) reporter;
    assertThat(impl.isConsumerThreadClose())
            .isTrue();
  }

  @Test(expected = IllegalStateException.class)
  public void flush_throwsOnClose() {
    reporter = AsyncReporter.builder(FakeSender.create())
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build();

    reporter.report(span);
    reporter.close(); // close while there's a pending span
    reporter.flush();
  }

  @Test
  public void report_doesntThrowWhenClosed() {
    reporter = AsyncReporter.builder(FakeSender.create())
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build();

    reporter.close();

    // Don't throw, as that could crash apps. Just increment drop metrics
    reporter.report(span);
    assertThat(metrics.spansDropped()).isEqualTo(1);
  }

  FakeSender sleepingSender = FakeSender.create().onSpans(spans -> {
    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  });

  @Test
  public void senderThread_dropsOnSenderClose_flushThread() throws InterruptedException {
    reporter = AsyncReporter.builder(sleepingSender)
        .metrics(metrics)
        .messageMaxBytes(sizeInBytesOfSingleSpanMessage)
        .messageTimeout(50, TimeUnit.MILLISECONDS)
        .build();

    reporter.report(span);
    Thread.sleep(1); // flush thread got the first span, but still waiting for more
    reporter.report(span);
    sleepingSender.close(); // close while there's a pending span
    Thread.sleep(10); // wait for the poll to unblock

    assertThat(metrics.spansDropped()).isEqualTo(1);
    assertThat(metrics.messagesDropped()).isEqualTo(1);
    assertThat(metrics.messagesDroppedByCause().keySet().iterator().next())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void senderThread_dropsOnReporterClose_flushThread() throws InterruptedException {
    CountDownLatch received = new CountDownLatch(1);
    CountDownLatch sent = new CountDownLatch(1);
    reporter = AsyncReporter.builder(FakeSender.create()
        .onSpans(spans -> {
          received.countDown();
          try {
            sent.await(); // block the flush thread
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }))
        .metrics(metrics)
        .messageMaxBytes(sizeInBytesOfSingleSpanMessage)
        .messageTimeout(50, TimeUnit.MILLISECONDS)
        .build();

    reporter.report(span);
    received.await(); // wait for the first span to send
    reporter.close(); // close while there's a pending span
    sent.countDown(); // release the flush thread

    assertThat(metrics.spansDropped()).isEqualTo(1);
  }

  @Test
  public void blocksToClearPendingSpans() throws InterruptedException {
    reporter = AsyncReporter.builder(FakeSender.create())
        .metrics(metrics)
        .messageTimeout(30, TimeUnit.SECONDS)
        .build();

    Thread.sleep(500); // wait for the thread to start

    reporter.report(span);
    reporter.close(); // close while there's a pending span

    assertThat(metrics.spans()).isEqualTo(1);
    assertThat(metrics.spansDropped()).isEqualTo(0);
  }

  @Test
  public void quitsBlockingWhenOverTimeout() throws InterruptedException {
    reporter = AsyncReporter.builder(FakeSender.create()
        .onSpans(spans -> {
          // note: we don't yet have a hook to cancel a sender, so this will remain in-flight
          // eventhough we are unblocking close. A later close on sender usually will kill in-flight
          try {
            Thread.sleep(1000); // block the flush thread longer than closeTimeout
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }))
        .metrics(metrics)
        .closeTimeout(1, TimeUnit.NANOSECONDS)
        .messageTimeout(30, TimeUnit.SECONDS)
        .build();

    Thread.sleep(500); // wait for the thread to start

    reporter.report(span);

    long start = System.nanoTime();
    reporter.close(); // close while there's a pending span
    assertThat(System.nanoTime() - start)
        .isLessThan(TimeUnit.MILLISECONDS.toNanos(100)); // give wiggle room
  }

  @Test
  public void flush_incrementsMetricsAndThrowsWhenClosed() throws InterruptedException {
    reporter = AsyncReporter.builder(sleepingSender)
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build();

    reporter.report(span);

    reporter.close(); // close() will report the queueing span but not sure the span can be sent
    try {
      reporter.flush();
      failBecauseExceptionWasNotThrown(IllegalStateException.class);
    } catch (IllegalStateException e) {
      assertThat(metrics.spans()).isEqualTo(1);
    }
  }

  @Test
  public void flush_incrementsMetricsAndThrowsWhenSenderClosed() throws InterruptedException {
    reporter = AsyncReporter.builder(sleepingSender)
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build();

    reporter.report(span);
    Thread.sleep(20);  // schedule consumer thread.

    sleepingSender.close();
    try {
      reporter.flush();
      failBecauseExceptionWasNotThrown(IllegalStateException.class);
    } catch (IllegalStateException e) {
      assertThat(metrics.spansDropped()).isEqualTo(1);
      assertThat(metrics.messagesDropped()).isEqualTo(1);
    }
  }
}
