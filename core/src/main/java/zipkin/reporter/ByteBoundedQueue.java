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

import com.lmax.disruptor.*;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.util.logging.Level.WARNING;


/**
 * Multi-producer, multi-consumer queue that is bounded by both count and size.
 *
 * <p>This uses {@link Disruptor} in implementation.
 */
final class ByteBoundedQueue implements EventHandler<SpanEvent>, TimeoutHandler, LifecycleAware{

  private static final Logger logger = Logger.getLogger(ByteBoundedQueue.class.getName());

  interface Consumer {
    /** Returns true if it accepted the next element */
    boolean accept(byte[] next);
  }

  private final Disruptor<SpanEvent> disruptor;
  private ExecutorService executor; // disruptor execute thread

  AtomicInteger sizeInBytes = new AtomicInteger(0);  // current size in bytes in queue
  AtomicInteger queuedSpans = new AtomicInteger(0);  // current spans in queue
  private final ReporterMetrics metrics;
  private final BufferNextMessage nextMessage;
  private final Sender sender;

  private final int maxBytes, maxSize;
  private final RingBuffer<SpanEvent> ringBuffer;  // ring buffer of disruptor
  private String oldThreadName; // remember the original thread name

  private AtomicBoolean close = new AtomicBoolean(true);

  ByteBoundedQueue(int maxSize, int maxBytes,
                   int messageMaxBytes, long messageTimeoutNanos,
                   ReporterMetrics metrics, Sender sender) {
    // the size of the ring buffer, must be power of 2.
    int ringBufferSize = 1 << (int) Math.ceil(Math.log(maxSize) / Math.log(2));

    // Executor that will be used to construct new threads for consumers
    this.executor = Executors.newFixedThreadPool(1, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread t = Executors.defaultThreadFactory().newThread(r);
        t.setDaemon(true);
        return t;
      }
    });

    // The factory for the event
    SpanEventFactory factory = new SpanEventFactory();

    // Flush every messageTimeoutNanos(defaults to 1 second)
    WaitStrategy waitStrategy;
    if (messageTimeoutNanos > 0)
      waitStrategy = new TimeoutBlockingWaitStrategy(messageTimeoutNanos, TimeUnit.NANOSECONDS);
    else
      waitStrategy = new BlockingWaitStrategy();

    // Construct the Disruptor
    this.disruptor = new Disruptor<>(factory, ringBufferSize, executor, ProducerType.MULTI, waitStrategy);

    // Connect the handler
    this.disruptor.handleEventsWith(this);

    // Start the Disruptor, starts all threads running
    this.disruptor.start();

    // Get the ring buffer from the Disruptor to be used for publishing.
    this.ringBuffer = this.disruptor.getRingBuffer();

    this.maxSize = maxSize;
    this.maxBytes = maxBytes;
    this.metrics = metrics;
    this.nextMessage = new BufferNextMessage(sender, messageMaxBytes, messageTimeoutNanos);
    this.sender = sender;
    this.close.set(false);
  }

  private static final EventTranslatorOneArg<SpanEvent, byte[]> TRANSLATOR =
          (event, sequence, bb) -> event.set(bb);

  /**
   * Returns true if the element could be added or false if it could not due to its size.
   */
  boolean offer(byte[] next) {
    if (sizeInBytes.intValue() + next.length > maxBytes)
      return false;
    if (queuedSpans.intValue() + 1 > maxSize)
      return false;
    boolean result = ringBuffer.tryPublishEvent(TRANSLATOR, next);

    if (result) {
      queuedSpans.incrementAndGet();
      sizeInBytes.addAndGet(next.length);
      metrics.updateQueuedSpans(queuedSpans.get());
      metrics.updateQueuedBytes(sizeInBytes.get());
    }

    return result;
  }

  /**
   * Shutdown disruptor and clear messages unconsumed.
   */
  void stop(final long timeout, final TimeUnit timeUnit) throws TimeoutException, InterruptedException {
    this.disruptor.shutdown(timeout, timeUnit);
    this.executor.awaitTermination(timeout, timeUnit);
    this.close.set(true);
  }

  /** Clears the queue unconditionally and returns count of elements cleared. */
  int clear() {
    int result = queuedSpans.get();
    queuedSpans.set(0);
    sizeInBytes.set(0);
    synchronized (nextMessage) {
      nextMessage.drain();
    }
    return result;
  }

  boolean is_close() {
    return this.close.get();
  }

  /**
   * Handle event. If batch is full or size reaches limit, flush the batch. Otherwise, add event to batch.
   */
  @Override
  public void onEvent(SpanEvent event, long sequence, boolean endOfBatch) throws Exception {
    synchronized (nextMessage) {
      if (!nextMessage.accept(event.getValue())) {
        flush();
        nextMessage.accept(event.getValue());
      }
    }
  }

  /**
   * Handle span callback.
   */
  private Callback sendSpansCallback(final int count, final int bytes) {
    return new Callback() {
      @Override public void onComplete() {
        updateQueueSizeAndBytes();
      }

      @Override public void onError(Throwable t) {
        updateQueueSizeAndBytes();
        metrics.incrementMessagesDropped(t);
        metrics.incrementSpansDropped(count);
        logger.log(WARNING,
                format("Dropped %s spans due to %s(%s)", count, t.getClass().getSimpleName(),
                        t.getMessage() == null ? "" : t.getMessage()), t);
      }

      private void updateQueueSizeAndBytes() {
        queuedSpans.addAndGet(-count);
        sizeInBytes.addAndGet(-bytes);
        metrics.updateQueuedSpans(queuedSpans.get());
        metrics.updateQueuedBytes(sizeInBytes.get());
      }
    };
  }

  /**
   * Called every {@link zipkin.reporter.AsyncReporter.BoundedAsyncReporter#messageTimeoutNanos}.
   */
  @Override
  public void onTimeout(long sequence) throws Exception {
    this.flush();
  }

  /**
   * Do flush batch if size reaches limit or flush interval comes.
   */
  void flush() {
    if (this.nextMessage.size() <= 0)
      return;

    metrics.incrementMessages();
    metrics.incrementMessageBytes(this.nextMessage.sizeInBytes());

    int size = this.nextMessage.size();
    int bytes = this.nextMessage.getRawBytes();

    List<byte[]> nextMessage = this.nextMessage.drain();

    // In failure case, we increment messages and spans dropped.
    // In complete case, we update size and bytes of current queue.
    Callback failureCallback = sendSpansCallback(size, bytes);
    try {
      sender.sendSpans(nextMessage, failureCallback);
    } catch (RuntimeException e) {
      failureCallback.onError(e);
      // Raise in case the sender was closed out-of-band.
      if (e instanceof IllegalStateException) throw e;
    }
  }

  @Override
  public void onStart() {
    final Thread currentThread = Thread.currentThread();
    oldThreadName = currentThread.getName();
    currentThread.setName("AsyncReporter(" + sender + ")");
  }

  @Override
  public void onShutdown() {
    this.flush();
    Thread.currentThread().setName(oldThreadName);
  }

}
