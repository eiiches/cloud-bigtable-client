package com.google.cloud.bigtable.grpc.io.loadbalancer;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.concurrent.GuardedBy;

public class LeastOutstandingReqeustsLoadBalancer<T> implements LoadBalancer<T> {
  private class RefCountedResource implements Resource<T> {
    private final T resource;

    @GuardedBy("pool")
    private int numOutstandingRequests = 0;

    public RefCountedResource(T resource) {
      this.resource = resource;
    }

    @Override
    public T get() {
      return resource;
    }

    @Override
    public void release() {
      synchronized (pool) {
        Set<RefCountedResource> slot = pool.get(numOutstandingRequests);

        // Remove resource from current slot
        slot.remove(this);
        if (slot.isEmpty()) {
          pool.remove(numOutstandingRequests);
        }

        // Put resource to new slot
        int newNumOutstandingRequests = --numOutstandingRequests;
        Set<RefCountedResource> newSlot = pool.get(newNumOutstandingRequests);
        if (newSlot == null) {
          newSlot = Collections.newSetFromMap(new LinkedHashMap<RefCountedResource, Boolean>());
          pool.put(newNumOutstandingRequests, newSlot);
        }
        newSlot.add(this);
      }
    }
  }

  /**
   * A TreeMap holding all the resources this load balancer has.
   *
   * Each resource is stored using the number of in-flight requests as a key so that less loaded
   * resources come first.
   *
   * Each map entry (slot) can hold multiple resources. They are also sorted so that lastly used
   * resources comes first, in order to provide a round-robin behavior among resources in the same
   * slot.
   */
  private final TreeMap<Integer, Set<RefCountedResource>> pool = new TreeMap<>();

  public LeastOutstandingReqeustsLoadBalancer(Collection<T> pool) {
    Set<RefCountedResource> resources = Collections.newSetFromMap(new LinkedHashMap<RefCountedResource, Boolean>());
    for (T object : pool) {
      resources.add(new RefCountedResource(object));
    }
    synchronized (this.pool) {
      // Initially there's no outstanding requests. All the resource goes to slot 0.
      this.pool.put(0, resources);
    }
  }

  @Override
  public Resource<T> next() {
    synchronized (pool) {
      Entry<Integer, Set<RefCountedResource>> slot = pool.firstEntry();
      Iterator<RefCountedResource> iter = slot.getValue().iterator();
      RefCountedResource resource = iter.next();

      // Remove resource from current slot
      iter.remove();
      if (slot.getValue().isEmpty()) { // If the slot becomes empty remove from map.
        pool.remove(slot.getKey());
      }

      // Put resource to new slot
      int newNumOutstandingRequests = ++resource.numOutstandingRequests;
      Set<RefCountedResource> newSlot = pool.get(newNumOutstandingRequests);
      if (newSlot == null) { // If the slot does not exist, create.
        newSlot = Collections.newSetFromMap(new LinkedHashMap<RefCountedResource, Boolean>());
        pool.put(newNumOutstandingRequests, newSlot);
      }
      newSlot.add(resource);

      return resource;
    }
  }

  public static class Factory<T> implements LoadBalancer.Factory<T> {
    @Override
    public LeastOutstandingReqeustsLoadBalancer<T> create(Collection<T> resources) {
      return new LeastOutstandingReqeustsLoadBalancer<>(resources);
    }
  }
}
