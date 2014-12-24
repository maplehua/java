package com.ruc.xx427.optimizer.model;

public class ShareRate {
	private float mapShareRate;
	private float reduceShareRate;

	public ShareRate(float mapSR, float reduceSR) {
		this.mapShareRate = mapSR;
		this.reduceShareRate = reduceSR;
	}

	public float getMapShareRate() {
		return mapShareRate;
	}

	public void setMapShareRate(float mapShareRate) {
		this.mapShareRate = mapShareRate;
	}

	public float getReduceShareRate() {
		return reduceShareRate;
	}

	public void setReduceShareRate(float reduceShareRate) {
		this.reduceShareRate = reduceShareRate;
	}

}
