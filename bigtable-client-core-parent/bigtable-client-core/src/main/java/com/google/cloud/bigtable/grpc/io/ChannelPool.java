/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.grpc.io;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.io.loadbalancer.LoadBalancer;
import com.google.cloud.bigtable.grpc.io.loadbalancer.RoundRobinLoadBalancer;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.Counter;
import com.google.cloud.bigtable.metrics.Meter;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;
import com.google.cloud.bigtable.metrics.Timer.Context;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptors.CheckedForwardingClientCall;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

/**
 * Manages a set of ClosableChannels and uses them in a round robin.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class ChannelPool extends ManagedChannel {

  /** Constant <code>LOG</code> */
  protected static final Logger LOG = new Logger(ChannelPool.class);

  /** Constant <code>CHANNEL_ID_KEY</code> */
  private static final Key<String> CHANNEL_ID_KEY =
      Key.of("bigtable-channel-id", Metadata.ASCII_STRING_MARSHALLER);

  public static final String extractIdentifier(Metadata trailers) {
    return trailers != null ? trailers.get(ChannelPool.CHANNEL_ID_KEY) : "";
  }

  /**
   * A factory for creating ManagedChannels to be used in a {@link ChannelPool}.
   *
   * @author sduskis
   *
   */
  public interface ChannelFactory {
    ManagedChannel create() throws IOException;
  }

  private static final AtomicInteger ChannelIdGenerator = new AtomicInteger();

  protected static Stats STATS;

  private static class Stats {
    /**
     * Best effort counter of active channels. There may be some cases where channel termination
     * counting may not accurately be decremented.
     */
    Counter ACTIVE_CHANNEL_COUNTER =
        BigtableClientMetrics.counter(MetricLevel.Info, "grpc.channel.active");

    /**
     * Best effort counter of active RPCs.
     */
    Counter ACTIVE_RPC_COUNTER = BigtableClientMetrics.counter(MetricLevel.Info, "grpc.rpc.active");

    /**
     * Best effort counter of RPCs.
     */
    Meter RPC_METER = BigtableClientMetrics.meter(MetricLevel.Info, "grpc.rpc.performed");
  }

  protected static synchronized Stats getStats() {
    if (STATS == null) {
      STATS = new Stats();
    }
    return STATS;
  }

  /**
   * Contains a {@link ManagedChannel} and metrics for the channel
   * @author sduskis
   *
   */
  private class InstrumentedChannel extends ManagedChannel {
    private final ManagedChannel delegate;
    // a uniquely named timer for this channel's latency
    private final Timer timer;

    private final AtomicBoolean active = new AtomicBoolean(true);
    private final int channelId;

    public InstrumentedChannel(ManagedChannel channel) {
      this.delegate = channel;
      this.channelId = ChannelIdGenerator.incrementAndGet();
      this.timer = BigtableClientMetrics.timer(MetricLevel.Trace,
        "channels.channel" + channelId + ".rpc.latency");
      getStats().ACTIVE_CHANNEL_COUNTER.inc();
    }

    private synchronized void markInactive(){
      boolean previouslyActive = active.getAndSet(false);
      if (previouslyActive) {
        getStats().ACTIVE_CHANNEL_COUNTER.dec();
      }
    }

    @Override
    public ManagedChannel shutdown() {
      markInactive();
      return delegate.shutdown();
    }

    @Override
    public boolean isShutdown() {
      return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
      return delegate.isTerminated();
    }

    @Override
    public ManagedChannel shutdownNow() {
      markInactive();
      return delegate.shutdownNow();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      markInactive();
      return delegate.awaitTermination(timeout, unit);
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT>
        newCall(MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions) {
      final Context timerContext = timer.time();
      final AtomicBoolean decremented = new AtomicBoolean(false);
      return new CheckedForwardingClientCall<ReqT, RespT>(delegate.newCall(methodDescriptor, callOptions)) {
        @Override
        protected void checkedStart(ClientCall.Listener<RespT> responseListener, Metadata headers)
            throws Exception {
          ClientCall.Listener<RespT> timingListener = wrap(responseListener, timerContext, decremented);
          getStats().ACTIVE_RPC_COUNTER.inc();
          getStats().RPC_METER.mark();
          delegate().start(timingListener, headers);
        }

        @Override
        public void cancel(String message, Throwable cause) {
          if (!decremented.getAndSet(true)) {
            getStats().ACTIVE_RPC_COUNTER.dec();
          }
          super.cancel(message, cause);
        }
      };
    }

    protected <RespT> ClientCall.Listener<RespT> wrap(final ClientCall.Listener<RespT> delegate,
        final Context timeContext, final AtomicBoolean decremented) {
      return new ClientCall.Listener<RespT>() {

        @Override
        public void onHeaders(Metadata headers) {
          delegate.onHeaders(headers);
        }

        @Override
        public void onMessage(RespT message) {
          delegate.onMessage(message);
        }

        @Override
        public void onClose(Status status, Metadata trailers) {
          try {
            if (trailers != null) {
              // Be extra defensive since this is only used for logging
              trailers.put(CHANNEL_ID_KEY, Integer.toString(channelId));
            }
            if (!decremented.getAndSet(true)) {
              getStats().ACTIVE_RPC_COUNTER.dec();
            }
            if (!status.isOk()) {
              BigtableClientMetrics.meter(MetricLevel.Info, "grpc.errors." + status.getCode().name())
                  .mark();
            }
            delegate.onClose(status, trailers);
          } finally {
            timeContext.close();
          }
        }

        @Override
        public void onReady() {
          delegate.onReady();
        }
      };
    }

    @Override
    public String authority() {
      return delegate.authority();
    }
  }

  private final ImmutableList<ManagedChannel> channels;
  private final String authority;

  private boolean shutdown = false;

  private final LoadBalancer<ManagedChannel> loadbalancer;

  public ChannelPool(ChannelFactory factory, int count) throws IOException {
    this(factory, count, new RoundRobinLoadBalancer.Factory<ManagedChannel>());
  }

  /**
   * <p>Constructor for ChannelPool.</p>
   *
   * @param factory a {@link com.google.cloud.bigtable.grpc.io.ChannelPool.ChannelFactory} object.
   * @throws java.io.IOException if any.
   */
  public ChannelPool(ChannelFactory factory, int count, LoadBalancer.Factory<ManagedChannel> loadBalancerFactory) throws IOException {
    Preconditions.checkArgument(count > 0, "Channel count has to be a positive number.");
    ImmutableList.Builder<ManagedChannel> channeListBuilder = ImmutableList.builder();
    for (int i = 0; i < count; i++) {
      channeListBuilder.add(new InstrumentedChannel(factory.create()));
    }
    this.channels = channeListBuilder.build();
    authority = channels.get(0).authority();
    this.loadbalancer = loadBalancerFactory.create(channels);
  }

  /** {@inheritDoc} */
  @Override
  public String authority() {
    return authority;
  }

  /**
   * {@inheritDoc}
   * <P>
   * Create a {@link ClientCall} on a Channel from the pool to the
   * remote operation specified by the given {@link MethodDescriptor}. The returned
   * {@link ClientCall} does not trigger any remote behavior until
   * {@link ClientCall#start(ClientCall.Listener, io.grpc.Metadata)} is invoked.
   */
  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions) {
    Preconditions.checkState(!shutdown, "Cannot perform operations on a closed connection");
    LoadBalancer.Resource<ManagedChannel> resource = loadbalancer.next();
    return new LoadBalancedClientCall<>(resource.get().newCall(methodDescriptor, callOptions), resource);
  }

  private static class LoadBalancedClientCall<ReqT, RespT> extends ForwardingClientCall<ReqT, RespT> {
    private final LoadBalancer.Resource<ManagedChannel> resource;
    private final AtomicBoolean completed = new AtomicBoolean();
    private final ClientCall<ReqT, RespT> delegate;

    private class ListenerWrapper extends Listener<RespT> {
      private final Listener<RespT> delegate;

      public ListenerWrapper(final Listener<RespT> delegate) {
        this.delegate = delegate;
      }

      @Override
      public void onClose(Status status, Metadata trailers) {
        if (completed.compareAndSet(false, true)) {
          resource.release();
        }
        delegate.onClose(status, trailers);
      }

      @Override
      public void onHeaders(Metadata headers) {
        delegate.onHeaders(headers);
      }

      @Override
      public void onMessage(RespT message) {
        delegate.onMessage(message);
      }

      @Override
      public void onReady() {
        delegate.onReady();
      }
    }

    public LoadBalancedClientCall(ClientCall<ReqT, RespT> delegate, final LoadBalancer.Resource<ManagedChannel> resource) {
      this.delegate = delegate;
      this.resource = resource;
    }

    @Override
    public void start(Listener<RespT> responseListener, Metadata headers) {
      super.start(new ListenerWrapper(responseListener), headers);
    }

    @Override
    protected ClientCall<ReqT, RespT> delegate() {
      return delegate;
    }

    @Override
    public void cancel(String message, Throwable cause) {
      if (completed.compareAndSet(false, true)) {
        resource.release();
      }
      super.cancel(message, cause);
    }
  }

  /**
   * <p>size.</p>
   *
   * @return a int.
   */
  public int size() {
    return channels.size();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized ManagedChannel shutdown() {
    for (ManagedChannel channelWrapper : channels) {
      channelWrapper.shutdown();
    }
    this.shutdown = true;
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isShutdown() {
    return shutdown;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isTerminated() {
    for (ManagedChannel channel : channels) {
      if (!channel.isTerminated()) {
        return false;
      }
    }
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public ManagedChannel shutdownNow() {
    for (ManagedChannel channel : channels) {
      if (!channel.isTerminated()) {
        channel.shutdownNow();
      }
    }
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    long endTimeNanos = System.nanoTime() + unit.toNanos(timeout);
    for (ManagedChannel channel : channels) {
      if (channel.isTerminated()) {
        continue;
      }
      long awaitTimeNanos = endTimeNanos - System.nanoTime();
      if (awaitTimeNanos <= 0) {
        break;
      }
      channel.awaitTermination(awaitTimeNanos, TimeUnit.NANOSECONDS);
    }

    return isTerminated();
  }
}
