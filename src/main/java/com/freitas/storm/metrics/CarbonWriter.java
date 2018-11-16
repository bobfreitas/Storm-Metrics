package com.freitas.storm.metrics;

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import com.google.common.base.Charsets;


public class CarbonWriter {
	
	private final String hostname;
	private final int port;

	private Socket socket;
	private OutputStream outStream;
	private boolean isClosed = false;
	
	public CarbonWriter(String host, int port) throws CarbonException {
		this.hostname = host;
		this.port = port;
		try {
			int i = 0;
			while ((this.socket = createSocket()) != null && i < 2) {
				this.outStream = this.socket.getOutputStream();
				i++;
			}
			if (this.outStream == null) {
				throw new CarbonException(String.format("Unable to create socket for to %s:%s", host, port));
			}
		} catch (IOException e) {
			throw new CarbonException(String.format("Connecting to %s:%s: %s", host, port, e.getMessage()), e);
		}
	}
	
	public void write(Object metric, Object value) throws CarbonException {
		write(metric, value, System.currentTimeMillis() / 1000);
	}
	
	public void close() {
		try {
			socket.close();
		} catch (IOException e) {
			// nothing can be done here
		}
	}

	public void write(Object metric, Object value, Number timestamp) throws CarbonException {
		checkState(!this.isClosed, "cannot write to closed object");

		try {
			this.outStream.write(format(metric, value, timestamp));
		}
		catch (IOException e) {
			throw new CarbonException(String.format("Writing to %s:%s: %s", this.hostname, this.port, e.getLocalizedMessage()), e);
		}
	}

	private byte[] format(Object metric, Object value, Number timestamp) {
		return String.format("%s %s %d%n", metric, value, timestamp).getBytes(Charsets.UTF_8);
	}
	
	protected Socket createSocket() throws IOException {
		Socket socket = null;
		try {
			socket = new Socket(this.hostname, this.port);
		}
		catch (Exception e) {
			// will retry above
		}
		return socket;
	}

}
