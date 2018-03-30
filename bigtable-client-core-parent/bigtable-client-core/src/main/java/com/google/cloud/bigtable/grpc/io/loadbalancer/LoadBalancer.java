package com.google.cloud.bigtable.grpc.io.loadbalancer;

import java.util.Collection;

public interface LoadBalancer<T> {
  interface Resource<T> {
    T get();

    void release();
  }

  interface Factory<T> {
    LoadBalancer<T> create(Collection<T> resources);
  }

  Resource<T> next();
}
