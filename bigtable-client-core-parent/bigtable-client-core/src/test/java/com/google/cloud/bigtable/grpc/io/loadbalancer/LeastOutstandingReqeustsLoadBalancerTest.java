package com.google.cloud.bigtable.grpc.io.loadbalancer;

import static org.junit.Assert.assertEquals;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

public class LeastOutstandingReqeustsLoadBalancerTest {
  @Test
  public void testLeastOutstandingRequests() {
    Set<Integer> items = new HashSet<>(Arrays.asList(1, 2, 3, 4, 5));
    LoadBalancer<Integer> loadbalancer = new LeastOutstandingReqeustsLoadBalancer<>(items);
    for (int i = 0; i < 10; ++i) {
      LoadBalancer.Resource<Integer> r1 = loadbalancer.next();
      LoadBalancer.Resource<Integer> r2 = loadbalancer.next();
      LoadBalancer.Resource<Integer> r3 = loadbalancer.next();
      LoadBalancer.Resource<Integer> r4 = loadbalancer.next();
      LoadBalancer.Resource<Integer> r5 = loadbalancer.next();
      Set<Integer> firstIteration = new HashSet<>(Arrays.asList(r1.get(), r2.get(), r3.get(), r4.get(), r5.get()));
      assertEquals(items, firstIteration);

      Integer v1 = r1.get();
      r1.release();
      r1 = loadbalancer.next();
      assertEquals(v1, r1.get());
    }
  }

  @Test
  public void testSequentialAccessShouldRoundRobin() {
    int numRequestPerServer = 10;
    int numServers = 5;

    Set<Integer> servers = new HashSet<>();
    for (int server = 0; server < numServers; ++server) {
      servers.add(server);
    }

    LoadBalancer<Integer> loadbalancer = new LeastOutstandingReqeustsLoadBalancer<>(servers);

    int[] requestCounts = new int[servers.size()];
    for (int i = 0; i < servers.size() * numRequestPerServer; ++i) {
      LoadBalancer.Resource<Integer> r = loadbalancer.next();
      requestCounts[r.get()]++;
      r.release();
    }

    for (int i = 0; i < servers.size(); ++i) {
      assertEquals(numRequestPerServer, requestCounts[i]);
    }
  }
}
