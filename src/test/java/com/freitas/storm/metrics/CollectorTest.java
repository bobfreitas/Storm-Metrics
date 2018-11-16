package com.freitas.storm.metrics;

import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import com.freitas.storm.metrics.Collector;
import com.freitas.storm.metrics.model.ClusterMetric;
import com.freitas.storm.metrics.model.Metric;

public class CollectorTest {
	
	private static String nimbusServer = "http://nimbus.mydomain.com:8080";
	
	@Ignore
	@Test
	public void testTopologyCollection() {
		System.out.println("Start test");
		
		Collector collector = new Collector();
		List<Metric> metrics = collector.collectToplogyMetrics(nimbusServer);
		
		System.out.println("Num topologies" + metrics.size());
		for (Metric metric: metrics) {
			System.out.println("Topology name: " + metric.getTopoName());
			System.out.println("    completeLatency: " + metric.getCompleteLatency());
			System.out.println("    avg capacity: " + metric.getAvgCapacity() + ", avg proc latency: " + metric.getAvgProcessLatency() + ", avg exec latency: " + metric.getAvgExecuteLatency());
			System.out.println("    avg transferred: " + metric.getAvgTransferred() + ", avg failed: " + metric.getAvgFailed() + "\n");
		}
		
		System.out.println("End test");
	}
	
	@Ignore
	@Test
	public void testClusterCollection() {
		System.out.println("Start test");
		
		Collector collector = new Collector();
		ClusterMetric metrics = collector.collectClusterMetrics(nimbusServer);
		System.out.println("Slots free: " + metrics.getSlotsFree());
		
		System.out.println("End test");
	}

}
