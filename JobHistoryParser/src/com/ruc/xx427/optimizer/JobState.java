package com.ruc.xx427.optimizer;

/**
 * define the state of a job
 * 
 * @author Juwei
 * 
 */
public class JobState {
	private JobStage jobStage;
	float progress;
	float diskShare;
	float netShare;
	float transTime;
	float shareRate;

	public JobState(JobStage jobStage, float progress, float diskShare,
			float netShare, float transTime, float shareRate) {
		this.jobStage = jobStage;
		this.progress = progress;
		this.diskShare = diskShare;
		this.netShare = netShare;
		this.transTime = transTime;
		this.shareRate = shareRate;
	}

	public JobStage getJobStage() {
		return jobStage;
	}

	public void setJobStage(JobStage jobStage) {
		this.jobStage = jobStage;
	}

	public float getProgress() {
		return progress;
	}

	public void setProgress(float progress) {
		this.progress = progress;
	}

	public float getDiskShare() {
		return diskShare;
	}

	public void setDiskShare(float diskShare) {
		this.diskShare = diskShare;
	}

	public float getNetShare() {
		return netShare;
	}

	public void setNetShare(float netShare) {
		this.netShare = netShare;
	}

	public float getTransTime() {
		return transTime;
	}

	public void setTransTime(float transTime) {
		this.transTime = transTime;
	}

	public float getShareRate() {
		return shareRate;
	}

	public void setShareRate(float shareRate) {
		this.shareRate = shareRate;
	}

}
