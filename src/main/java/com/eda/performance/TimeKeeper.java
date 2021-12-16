package com.eda.performance;

public class TimeKeeper {
	
	private Long start;
	private Long end;
	private Long diff;
	
	public TimeKeeper() {
		start = System.currentTimeMillis();
	}
	
	public void start() {
		this.start = System.currentTimeMillis();
	}
	
	public Long stop() {
		this.end = System.currentTimeMillis();
		this.diff = this.end - this.start;
		return diff;
	}
	
	public Long getDiff() {
		return diff;
	}
	
	public String getDiffString() {
		return Long.toString(this.diff);
	}

}
