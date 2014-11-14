package com.ruc.xx427.optimizer;

public class JobFactors {

	private int mapNum;

	private int reduceNum;

	private boolean mapCompress;

	private boolean jobCompress;

	private float slowStart;

	public int getMapNum() {
		return mapNum;
	}

	public void setMapNum(int mapNum) {
		this.mapNum = mapNum;
	}

	public int getReduceNum() {
		return reduceNum;
	}

	public void setReduceNum(int reduceNum) {
		this.reduceNum = reduceNum;
	}

	public boolean isMapCompress() {
		return mapCompress;
	}

	public void setMapCompress(boolean mapCompress) {
		this.mapCompress = mapCompress;
	}

	public boolean isJobCompress() {
		return jobCompress;
	}

	public void setJobCompress(boolean jobCompress) {
		this.jobCompress = jobCompress;
	}

	public float getSlowStart() {
		return slowStart;
	}

	public void setSlowStart(float slowStart) {
		this.slowStart = slowStart;
	}
	
	

}
