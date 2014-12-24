package com.ruc.xx427.profile.rsc;
/**
 * 
 * Description: the system profile of the cluster
 *
 * @author Juwei
 * @date 2014-11-14
 *
 */
public class ResourceProfile {
	
	private float scheduleTime; // sec
	
	private float diskReadTP; // MB/sec
	
	private float diskWriteTP; // MB/sec
	
	private float netTP; // MB/sec

	public float getScheduleTime() {
		return scheduleTime;
	}

	public void setScheduleTime(float scheduleTime) {
		this.scheduleTime = scheduleTime;
	}

	public float getDiskReadTP() {
		return diskReadTP;
	}

	public void setDiskReadTP(float diskReadTP) {
		this.diskReadTP = diskReadTP;
	}

	public float getDiskWriteTP() {
		return diskWriteTP;
	}

	public void setDiskWriteTP(float diskWriteTP) {
		this.diskWriteTP = diskWriteTP;
	}

	public float getNetTP() {
		return netTP;
	}

	public void setNetTP(float netTP) {
		this.netTP = netTP;
	}
	
	// we use maptime+compress and reduce+decompress in profiles
	//private double compressTP; // MB/sec
	//private double decompressTP; // MB/sec


	
}
