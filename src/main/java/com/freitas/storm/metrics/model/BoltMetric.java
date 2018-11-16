package com.freitas.storm.metrics.model;

public class BoltMetric {
	
	private String boltId;
	private Float capacity;
	private Float processLatency;
	private Float executeLatency;
	private Integer transferred;
	private Integer failed;
	
	public String getBoltId() {
		return boltId;
	}
	public void setBoltId(String boltId) {
		this.boltId = boltId;
	}
	
	public Float getCapacity() {
		return capacity;
	}
	public void setCapacity(Float capacity) {
		this.capacity = capacity;
	}
	
	public Float getProcessLatency() {
		return processLatency;
	}
	public void setProcessLatency(Float processLatency) {
		this.processLatency = processLatency;
	}
	
	public Float getExecuteLatency() {
		return executeLatency;
	}
	public void setExecuteLatency(Float executeLatency) {
		this.executeLatency = executeLatency;
	}
	
	public Integer getTransferred() {
		return transferred;
	}
	public void setTransferred(Integer transferred) {
		this.transferred = transferred;
	}
	
	public Integer getFailed() {
		return failed;
	}
	public void setFailed(Integer failed) {
		this.failed = failed;
	}

}
