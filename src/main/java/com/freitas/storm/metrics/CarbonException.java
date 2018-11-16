package com.freitas.storm.metrics;

public class CarbonException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public CarbonException(String msg) {
		super(msg);
	}

	public CarbonException(Throwable cause) {
		super(cause.getLocalizedMessage(), cause);
	}

	public CarbonException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
