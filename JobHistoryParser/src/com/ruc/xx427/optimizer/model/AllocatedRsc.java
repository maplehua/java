package com.ruc.xx427.optimizer.model;

public class AllocatedRsc {
	
	private float diskWritePerTaskTP;
	
	private float diskReadPerTaskTP;
	
	private float netPerTaskTP;
	

	public float getDiskWritePerTaskTP() {
		return diskWritePerTaskTP;
	}

	public void setDiskWritePerTaskTP(float diskWritePerTaskTP) {
		this.diskWritePerTaskTP = diskWritePerTaskTP;
	}

	public float getDiskReadPerTaskTP() {
		return diskReadPerTaskTP;
	}

	public void setDiskReadPerTaskTP(float diskReadPerTaskTP) {
		this.diskReadPerTaskTP = diskReadPerTaskTP;
	}

	public float getNetPerTaskTP() {
		return netPerTaskTP;
	}

	public void setNetPerTaskTP(float netPerTaskTP) {
		this.netPerTaskTP = netPerTaskTP;
	}
	

}
