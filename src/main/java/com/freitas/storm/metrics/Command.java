package com.freitas.storm.metrics;

import java.util.List;

import com.freitas.storm.metrics.model.ClusterMetric;
import com.freitas.storm.metrics.model.Metric;

public class Command {

	public static void main(String... args) throws Exception {
		if (args.length != 4) {
			System.err.printf("Usage: %s <nimbus server> <graphite host> <graphite port> <prefix>%n", Command.class.getSimpleName());
			System.exit(1);
		}

		Integer graphitePort = null;
		try {
			graphitePort = Integer.parseInt(args[2]);
		}
		catch (NumberFormatException e) {
			System.err.println("Not a valid port number: " + args[2]);
			System.exit(1);
		}
		
		// collect the metrics 
		Collector collector = new Collector();
		List<Metric> topoMetrics = collector.collectToplogyMetrics(args[0]);
		if (topoMetrics == null) {
			throw new Exception("Unable to collect topology metrics from " + args[0]);
		}
		ClusterMetric clusterMetrics = collector.collectClusterMetrics(args[0]);
		if (clusterMetrics == null) {
			throw new Exception("Unable to collect cluster metrics from " + args[0]);
		}
		
		//write the metrics to carbon
		CarbonWriter writer = new CarbonWriter(args[1], graphitePort);
		Formatter formatter = new Formatter();
		formatter.formatClusterMetrics(clusterMetrics, writer, args[3]);
		formatter.formatTopoMetrics(topoMetrics, writer, args[3]);
		writer.close();

	}

}
