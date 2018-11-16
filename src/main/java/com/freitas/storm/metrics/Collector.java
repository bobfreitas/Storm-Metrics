package com.freitas.storm.metrics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import com.freitas.storm.metrics.model.BoltMetric;
import com.freitas.storm.metrics.model.ClusterMetric;
import com.freitas.storm.metrics.model.Metric;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class Collector {
	
	private static final String SPOUTS = "spouts";
	private static final String BOLTS = "bolts";
	private static final String CAPACITY = "capacity";
	private static final String PROCESS_LATENCY = "processLatency";
	private static final String EXECUTE_LATENCY = "executeLatency";
	private static final String TRANSFERRED = "transferred";
	private static final String FAILED = "failed";
	private static final String COMPLETE_LATENCY = "completeLatency";
	private static final String NAME = "name";
	private static final String BOLT_ID = "boltId";
	private static final List<String> BOLT_PROPS = Arrays.asList(CAPACITY, PROCESS_LATENCY, EXECUTE_LATENCY, TRANSFERRED, FAILED);
	private static final String SLOTS_FREE = "slotsFree";
	
	public ClusterMetric collectClusterMetrics(String nimbusServer) {
		HttpClientBuilder builder = HttpClientBuilder.create();
		HttpClientContext context = HttpClientContext.create();
		Gson gson = new Gson();
		HttpGet request = new HttpGet(nimbusServer + "/api/v1/cluster/summary");
		CloseableHttpClient client = builder.build();
		ClusterMetric metric = new ClusterMetric();
		try {
			HttpResponse response = client.execute(request, context);
			if (response.getStatusLine().getStatusCode() == 200) {
				HttpEntity entity = response.getEntity();
				String result = EntityUtils.toString(entity);
				JsonObject clusterSummary = gson.fromJson(result, JsonObject.class);
				JsonElement slotElement = clusterSummary.get(SLOTS_FREE);
				Integer slotsFree = slotElement.getAsInt();
				metric.setSlotsFree(slotsFree);
			}
			else {
				return null;
			}
		}
		catch (Exception e) {
			return null;
		}
		
		return metric;
	}
	
	public List<Metric> collectToplogyMetrics(String nimbusServer) {
		HttpClientBuilder builder = HttpClientBuilder.create();
		HttpClientContext context = HttpClientContext.create();
		Gson gson = new Gson();
		HttpGet request = new HttpGet(nimbusServer + "/api/v1/topology/summary");
		CloseableHttpClient client = builder.build();
		List<Metric> metricList = new ArrayList<Metric>(25);
		try {
			HttpResponse response = client.execute(request, context);
			if (response.getStatusLine().getStatusCode() == 200) {
				HttpEntity entity = response.getEntity();
				String result = EntityUtils.toString(entity);
				JsonObject topologySummary = gson.fromJson(result, JsonObject.class);
				List<String> ids = extractTopologyIds(topologySummary.get("topologies").getAsJsonArray());
				if (ids.isEmpty()) {
					client.close();
					return null;
				}
				for (String id : ids) {
					Metric metric = new Metric();
					List<BoltMetric> boltMetrics = new ArrayList<BoltMetric>(50);
					metric.setBoltMetrics(boltMetrics);
					metricList.add(metric);
					fetchTopologyMetrics(nimbusServer, id, builder, context, gson, metric);
				}
			} else {
				client.close();
				return null;
			}
			client.close();
		} catch (Exception e) {
			return null;
		}
		
		return metricList;
	}
	
	private List<String> extractTopologyIds(JsonArray topologies) {
		List<String> topologyIds = new ArrayList<>();
		for (JsonElement topologyElement : topologies) {
			JsonObject topologyObject = topologyElement.getAsJsonObject();
			if (topologyObject.has("id")) {
				String topologyId = topologyObject.get("id").getAsString();
				topologyIds.add(topologyId);
			}
		}
		return topologyIds;
	}
	
	private void fetchTopologyMetrics(String nimbusServer, String topologyId, HttpClientBuilder builder, 
			HttpClientContext context, Gson gson, Metric metric) throws ClientProtocolException, IOException {
		
		CloseableHttpClient client = builder.build();
		HttpGet get = new HttpGet(nimbusServer + "/api/v1/topology/" + topologyId + "?window=600");
		CloseableHttpResponse result = client.execute(get, context);
		if (result.getStatusLine().getStatusCode() == 200) {
			String metrics = EntityUtils.toString(result.getEntity());
			JsonObject topologyMetrics = gson.fromJson(metrics, JsonObject.class);
			JsonElement nameElement = topologyMetrics.get(NAME);
			String name = nameElement.getAsString();
			metric.setTopoName(name);
			if (topologyMetrics.has(SPOUTS)) {
				// there will only be one spout
				JsonArray spouts = topologyMetrics.get(SPOUTS).getAsJsonArray();
				JsonElement spoutElement = spouts.get(0);
				JsonObject spout = spoutElement.getAsJsonObject();
				Float latencyValue = spout.get(COMPLETE_LATENCY).getAsFloat();
				metric.setCompleteLatency(latencyValue);
			}
			if (topologyMetrics.has(BOLTS)) {
				JsonArray bolts = topologyMetrics.get(BOLTS).getAsJsonArray();
				for (JsonElement boltElement : bolts) {
					JsonObject bolt = boltElement.getAsJsonObject();
					BoltMetric boltMetric = new BoltMetric();
					String boltId = bolt.get(BOLT_ID).getAsString();
					boltMetric.setBoltId(boltId);
					for (String field : BOLT_PROPS) {
						switch (field) {
							case CAPACITY:
								Float capacity = bolt.get(field).getAsFloat();
								boltMetric.setCapacity(capacity);
								break;
							case PROCESS_LATENCY:
								Float procLatency = bolt.get(field).getAsFloat();
								boltMetric.setProcessLatency(procLatency);
								break;
							case EXECUTE_LATENCY:
								Float execLatency = bolt.get(field).getAsFloat();
								boltMetric.setExecuteLatency(execLatency);
								break;
							case TRANSFERRED:
								Integer transferred = bolt.get(field).getAsInt();
								boltMetric.setTransferred(transferred);
								break;
							case FAILED:
								Integer failed = bolt.get(field).getAsInt();
								boltMetric.setFailed(failed);
								break;
							default:
							// ignore, not what we need
						}
					}
					metric.getBoltMetrics().add(boltMetric);
				}
			}
		} else {
			return;
		}
	}

}
