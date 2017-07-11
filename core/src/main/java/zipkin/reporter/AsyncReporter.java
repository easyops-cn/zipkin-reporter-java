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

import com.lmax.disruptor.TimeoutException;

import java.io.Flushable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import zipkin.Component;
import zipkin.Span;

import static zipkin.internal.Util.checkArgument;
import static zipkin.internal.Util.checkNotNull;

/**
 * As spans are reported, they are encoded and added to a pending queue. The task of sending spans
 * happens on a separate disruptor thread calls {@link ByteBoundedQueue#onEvent(SpanEvent, long, boolean)} ()}.
 * By doing so, callers are protected
 * from latency or exceptions possible when exporting spans out of process.
 *
 * <p>Spans are bundled into messages based on size in bytes or a timeout, whichever happens first.
 *
 * @param <S> type of the span, usually {@link zipkin.Span}
 */
public abstract class AsyncReporter<S> implements Reporter<S>, Flushable, Component {
  /**
   * After a certain threshold, spans are drained and {@link Sender#sendSpans(List, Callback) sent}
   * to Zipkin collectors.
   */
  public static AsyncReporter<Span> create(Sender sender) {
    return new Builder(sender).build();
  }

  /** Like {@link #create(Sender)}, except you can configure settings such as the timeout. */
  public static Builder builder(Sender sender) {
    return new Builder(sender);
  }

  /**
   * Calling this will flush any pending spans to the transport on the current thread.
   *
   * <p>Note: If you set {@link Builder#messageTimeout(long, TimeUnit) message timeout} to zero, you
   * must call this externally as otherwise spans will never be sent.
   *
   * @throws IllegalStateException if closed
   */
  @Override public abstract void flush();

  /** Shuts down the sender thread, and increments drop metrics if there were any unsent spans. */
  @Override public abstract void close();

  public static final class Builder {
    final Sender sender;
    ReporterMetrics metrics = ReporterMetrics.NOOP_METRICS;
    int messageMaxBytes;
    long messageTimeoutNanos = TimeUnit.SECONDS.toNanos(1);
    long closeTimeoutNanos = TimeUnit.SECONDS.toNanos(1);
    int queuedMaxSpans = 10000;
    int queuedMaxBytes = onePercentOfMemory();

    static int onePercentOfMemory() {
      long result = (long) (Runtime.getRuntime().totalMemory() * 0.01);
      // don't overflow in the rare case 1% of memory is larger than 2 GiB!
      return (int) Math.max(Math.min(Integer.MAX_VALUE, result), Integer.MIN_VALUE);
    }

    Builder(Sender sender) {
      this.sender = checkNotNull(sender, "sender");
      this.messageMaxBytes = sender.messageMaxBytes();
    }

    /**
     * Aggregates and reports reporter metrics to a monitoring system. Defaults to no-op.
     */
    public Builder metrics(ReporterMetrics metrics) {
      this.metrics = checkNotNull(metrics, "metrics");
      return this;
    }

    /**
     * Maximum bytes sendable per message including overhead. Defaults to, and is limited by {@link
     * Sender#messageMaxBytes()}.
     */
    public Builder messageMaxBytes(int messageMaxBytes) {
      checkArgument(messageMaxBytes >= 0, "messageMaxBytes < 0: %s", messageMaxBytes);
      this.messageMaxBytes = Math.min(messageMaxBytes, sender.messageMaxBytes());
      return this;
    }

    /**
     * Default 1 second. 0 implies spans are {@link ByteBoundedQueue#flush() flushed} externally.
     *
     * <p>Instead of sending one message at a time, spans are bundled into messages, up to {@link
     * Sender#messageMaxBytes()}. This timeout ensures that spans are not stuck in an incomplete
     * message.
     *
     * <p>Note: this timeout starts when the first unsent span is reported.
     */
    public Builder messageTimeout(long timeout, TimeUnit unit) {
      checkArgument(timeout >= 0, "messageTimeout < 0: %s", timeout);
      this.messageTimeoutNanos = unit.toNanos(checkNotNull(timeout, "messageTimeout"));
      return this;
    }

    /** How long to block for in-flight spans to send out-of-process on close. Default 1 second */
    public Builder closeTimeout(long timeout, TimeUnit unit) {
      checkArgument(timeout >= 0, "closeTimeout < 0: %s", timeout);
      this.closeTimeoutNanos = unit.toNanos(checkNotNull(timeout, "closeTimeout"));
      return this;
    }

    /** Maximum backlog of spans reported vs sent. Default 10000 */
    public Builder queuedMaxSpans(int queuedMaxSpans) {
      this.queuedMaxSpans = queuedMaxSpans;
      return this;
    }

    /** Maximum backlog of span bytes reported vs sent. Default 1% of heap */
    public Builder queuedMaxBytes(int queuedMaxBytes) {
      this.queuedMaxBytes = queuedMaxBytes;
      return this;
    }

    /** Builds an async reporter that encodes zipkin spans as they are reported. */
    public AsyncReporter<Span> build() {
      switch (sender.encoding()) {
        case JSON:
          return build(Encoder.JSON);
        case THRIFT:
          return build(Encoder.THRIFT);
        default:
          throw new UnsupportedOperationException(sender.encoding().name());
      }
    }

    /** Builds an async reporter that encodes arbitrary spans as they are reported. */
    public <S> AsyncReporter<S> build(Encoder<S> encoder) {
      checkNotNull(encoder, "encoder");
      checkArgument(encoder.encoding() == sender.encoding(),
          "Encoder.encoding() %s != Sender.encoding() %s",
          encoder.encoding(), sender.encoding());

      final BoundedAsyncReporter<S> result = new BoundedAsyncReporter<>(this, encoder);
      return result;
    }
  }

  static final class BoundedAsyncReporter<S> extends AsyncReporter<S> {
    static final Logger logger = Logger.getLogger(BoundedAsyncReporter.class.getName());
    final AtomicBoolean closed = new AtomicBoolean(false);
    final Encoder<S> encoder;
    final ByteBoundedQueue pending;
    final Sender sender;
    final int messageMaxBytes;
    final long messageTimeoutNanos;
    final long closeTimeoutNanos;
    final ReporterMetrics metrics;

    BoundedAsyncReporter(Builder builder, Encoder<S> encoder) {
      this.sender = builder.sender;
      this.messageMaxBytes = builder.messageMaxBytes;
      this.messageTimeoutNanos = builder.messageTimeoutNanos;
      this.closeTimeoutNanos = builder.closeTimeoutNanos;
      this.metrics = builder.metrics;
      this.encoder = encoder;
      this.pending = new ByteBoundedQueue(builder.queuedMaxSpans, builder.queuedMaxBytes,
              builder.messageMaxBytes, builder.messageTimeoutNanos, metrics,
              sender);
    }

    /** Returns true if the was encoded and accepted onto the queue. */
    @Override
    public void report(S span) {
      checkNotNull(span, "span");
      metrics.incrementSpans(1);
      byte[] next = encoder.encode(span);
      int messageSizeOfNextSpan = sender.messageSizeInBytes(Collections.singletonList(next));
      metrics.incrementSpanBytes(next.length);
      if (closed.get() ||
          // don't enqueue something larger than we can drain
          messageSizeOfNextSpan > messageMaxBytes ||
          !pending.offer(next)) {
        metrics.incrementSpansDropped(1);
      }
    }

    @Override public CheckResult check() {
      return sender.check();
    }

    @Override
    public void close() {
      if (!closed.compareAndSet(false, true)) return; // already closed
      try {
        // wait for in-flight spans to send
        pending.stop(closeTimeoutNanos, TimeUnit.NANOSECONDS);
      } catch (TimeoutException e) {
        logger.warning("Timed out waiting for in-flight spans to send");
      } catch (InterruptedException e) {
        logger.warning("Interrupted waiting for in-flight spans to send");
        Thread.currentThread().interrupt();
      }
      int count = pending.clear();
      if (count > 0) {
        metrics.incrementSpansDropped(count);
        logger.warning("Dropped " + count + " spans due to AsyncReporter.close()");
      }
    }

    @Override public String toString() {
      return "AsyncReporter(" + sender + ")";
    }

    @Override
    public final void flush() {
      if (closed.get()) throw new IllegalStateException("closed");
      this.pending.flush();
    }

    public final boolean isConsumerThreadClose() {
      return this.pending.is_close();
    }
  }
}
