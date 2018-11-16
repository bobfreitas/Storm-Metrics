package com.freitas.storm.metrics.model;

import java.util.List;

public class Metric {
	
	private String topoName;
	private Float completeLatency;
	private List<BoltMetric> boltMetrics;
	
	public String getTopoName() {
		return topoName;
	}
	public void setTopoName(String topoName) {
		this.topoName = topoName;
	}
	
	public Float getCompleteLatency() {
		return completeLatency;
	}
	public void setCompleteLatency(Float completeLatency) {
		this.completeLatency = completeLatency;
	}
	
	public List<BoltMetric> getBoltMetrics() {
		return boltMetrics;
	}
	public void setBoltMetrics(List<BoltMetric> boltMetrics) {
		this.boltMetrics = boltMetrics;
	}
	
	public Float getAvgCapacity() {
		int count = 0;
		float sum = 0.0f;
		for (BoltMetric boltMetric: boltMetrics) {
			if (boltMetric.getCapacity().compareTo(0.0f) != 0) {
				sum =+ boltMetric.getCapacity();
				count++;
			}
		}
		try {
			if (count == 0) {
				return 0.0f;
			}
			return sum/count;
		}
		catch (Exception e) {
			return 0.0f;
		}
	}
	
	public Float getAvgProcessLatency() {
		int count = 0;
		float sum = 0.0f;
		for (BoltMetric boltMetric: boltMetrics) {
			if (boltMetric.getProcessLatency().compareTo(0.0f) != 0) {
				sum =+ boltMetric.getProcessLatency();
				count++;
			}
		}
		try {
			if (count == 0) {
				return 0.0f;
			}
			return sum/count;
		}
		catch (Exception e) {
			return 0.0f;
		}
	}
	
	public Float getAvgExecuteLatency() {
		int count = 0;
		float sum = 0.0f;
		for (BoltMetric boltMetric: boltMetrics) {
			if (boltMetric.getExecuteLatency().compareTo(0.0f) != 0) {
				sum =+ boltMetric.getExecuteLatency();
				count++;
			}
		}
		try {
			if (count == 0) {
				return 0.0f;
			}
			return sum/count;
		}
		catch (Exception e) {
			return 0.0f;
		}
	}
	
	public Integer getAvgTransferred() {
		int count = 0;
		int sum = 0;
		for (BoltMetric boltMetric: boltMetrics) {
			if (boltMetric.getTransferred().compareTo(0) != 0) {
				sum =+ boltMetric.getTransferred();
				count++;
			}
		}
		try {
			if (count == 0) {
				return 0;
			}
			return sum/count;
		}
		catch (Exception e) {
			return 0;
		}
	}
	
	public Integer getAvgFailed() {
		int count = 0;
		int sum = 0;
		for (BoltMetric boltMetric: boltMetrics) {
			if (boltMetric.getFailed().compareTo(0) != 0) {
				sum =+ boltMetric.getFailed();
				count++;
			}
		}
		try {
			if (count == 0) {
				return 0;
			}
			return sum/count;
		}
		catch (Exception e) {
			return 0;
		}
	}

}
