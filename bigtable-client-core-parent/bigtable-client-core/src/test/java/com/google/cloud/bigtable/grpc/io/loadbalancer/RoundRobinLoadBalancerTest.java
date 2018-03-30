package com.google.cloud.bigtable.grpc.io.loadbalancer;

import static org.junit.Assert.assertEquals;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

public class RoundRobinLoadBalancerTest {

  @Test
  public void testRoundRobin() {
    int numRequestPerServer = 10;
    int numServers = 5;

    Set<Integer> servers = new HashSet<>();
    for (int server = 0; server < numServers; ++server) {
      servers.add(server);
    }

    LoadBalancer<Integer> loadbalancer = new RoundRobinLoadBalancer<>(servers);

    int[] requestCounts = new int[servers.size()];
    for (int i = 0; i < servers.size() * numRequestPerServer; ++i) {
      LoadBalancer.Resource<Integer> r = loadbalancer.next();
      requestCounts[r.get()]++;
    }

    for (int i = 0; i < servers.size(); ++i) {
      assertEquals(numRequestPerServer, requestCounts[i]);
    }
  }
}
