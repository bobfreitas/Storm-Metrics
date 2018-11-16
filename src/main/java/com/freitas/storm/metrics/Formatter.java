package com.freitas.storm.metrics;

import java.util.List;

import com.freitas.storm.metrics.model.BoltMetric;
import com.freitas.storm.metrics.model.ClusterMetric;
import com.freitas.storm.metrics.model.Metric;

public class Formatter {
	
	public void formatClusterMetrics(ClusterMetric clusterMetrics, CarbonWriter writer, String prefix) {
		StringBuilder sb = new StringBuilder(256);
		sb.append(prefix);
		sb.append(".storm-cluster.gauge_slotsFree");
		writer.write(sb.toString(), clusterMetrics.getSlotsFree());
	}
	
	public void formatTopoMetrics(List<Metric> topoMetrics, CarbonWriter writer, String prefix) {
		for (Metric metric : topoMetrics) {
			writeSingleTopoMetric(metric, writer, prefix, "completeLatency", metric.getCompleteLatency());
			writeSingleTopoMetric(metric, writer, prefix, "avgCapacity", metric.getAvgCapacity());
			writeSingleTopoMetric(metric, writer, prefix, "avgProcessLatency", metric.getAvgProcessLatency());
			writeSingleTopoMetric(metric, writer, prefix, "avgExecuteLatency", metric.getAvgExecuteLatency());
			writeSingleTopoMetric(metric, writer, prefix, "avgTransferred", metric.getAvgTransferred());
			writeSingleTopoMetric(metric, writer, prefix, "avgFailed", metric.getAvgFailed());
			for (BoltMetric boltMetric: metric.getBoltMetrics()) {
				writeSingleBoltMetric(metric, boltMetric, writer, prefix, "capacity", boltMetric.getCapacity());
				writeSingleBoltMetric(metric, boltMetric, writer, prefix, "processLatency", boltMetric.getProcessLatency());
				writeSingleBoltMetric(metric, boltMetric, writer, prefix, "executeLatency", boltMetric.getExecuteLatency());
				writeSingleBoltMetric(metric, boltMetric, writer, prefix, "transferred", boltMetric.getTransferred());
				writeSingleBoltMetric(metric, boltMetric, writer, prefix, "failed", boltMetric.getFailed());
			}
		}
	}
	
	private void writeSingleTopoMetric(Metric metric, CarbonWriter writer, String prefix, String metricName, Number metricValue) {
		StringBuilder sb = new StringBuilder(256);
		sb.append(prefix);
		sb.append(".storm-");
		sb.append(metric.getTopoName());
		sb.append(".gauge-topo_");
		sb.append(metricName);
		writer.write(sb.toString(), metricValue);
	}
	
	private void writeSingleBoltMetric(Metric metric, BoltMetric boltMetric, CarbonWriter writer, String prefix, String metricName, Number metricValue) {
		StringBuilder sb = new StringBuilder(256);
		sb.append(prefix);
		sb.append(".storm-");
		sb.append(metric.getTopoName());
		sb.append(".gauge-bolt-");
		sb.append(boltMetric.getBoltId());
		sb.append("_");
		sb.append(metricName);
		writer.write(sb.toString(), metricValue);
	}

}
