package com.ruc.xx427.optimizer.model;

/**
 * define the state of a job
 * 
 * @author Juwei
 * 
 */
public class JobState {
	private JobStage jobStage;
	//slots and progress
	ProgressTracker progressTracker;
	//resource share
	AllocatedRsc mapShare;
	AllocatedRsc reduceShare;
	float transTime;
	
	
	public JobState(JobStage jobStage, ProgressTracker progressTracker,  AllocatedRsc mapShare, float transTime) {
		this.jobStage = jobStage;
		this.progressTracker = progressTracker;
		this.mapShare = mapShare; // job start from map 		
		this.transTime = transTime;
	}


	public JobStage getJobStage() {
		return jobStage;
	}


	public void setJobStage(JobStage jobStage) {
		this.jobStage = jobStage;
	}


	public ProgressTracker getProgressTracker() {
		return progressTracker;
	}


	public void setProgressTracker(ProgressTracker progressTracker) {
		this.progressTracker = progressTracker;
	}


	public AllocatedRsc getMapShare() {
		return mapShare;
	}


	public void setMapShare(AllocatedRsc mapShare) {
		this.mapShare = mapShare;
	}


	public AllocatedRsc getReduceShare() {
		return reduceShare;
	}


	public void setReduceShare(AllocatedRsc reduceShare) {
		this.reduceShare = reduceShare;
	}


	public float getTransTime() {
		return transTime;
	}


	public void setTransTime(float transTime) {
		this.transTime = transTime;
	}




}
