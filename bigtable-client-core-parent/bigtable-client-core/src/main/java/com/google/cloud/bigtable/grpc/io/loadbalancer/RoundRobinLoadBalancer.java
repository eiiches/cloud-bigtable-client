package com.google.cloud.bigtable.grpc.io.loadbalancer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinLoadBalancer<T> implements LoadBalancer<T> {
  private final AtomicInteger requestCount = new AtomicInteger();
  private final List<Resource<T>> resources;

  private static class SimpleResource<T> implements Resource<T> {
    private final T resource;

    public SimpleResource(T resource) {
      this.resource = resource;
    }

    @Override
    public void release() {}

    @Override
    public T get() {
      return resource;
    }
  }

  public RoundRobinLoadBalancer(Collection<T> resources) {
    this.resources = new ArrayList<>(resources.size());
    for (T resource : resources) {
      this.resources.add(new SimpleResource<>(resource));
    }
  }

  /**
   * Performs a simple round robin. This method should not be synchronized, if possible,
   * to reduce bottlenecks.
   *
   * @return A {@link Resource} that can be used for a single call.
   */
  @Override
  public Resource<T> next() {
    int currentRequestNum = requestCount.getAndIncrement();
    int index = Math.abs(currentRequestNum % resources.size());
    return resources.get(index);
  }

  public static class Factory<T> implements LoadBalancer.Factory<T> {
    @Override
    public RoundRobinLoadBalancer<T> create(Collection<T> resources) {
      return new RoundRobinLoadBalancer<>(resources);
    }
  }
}
