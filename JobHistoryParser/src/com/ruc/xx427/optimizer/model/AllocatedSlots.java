package com.ruc.xx427.optimizer.model;

public class AllocatedSlots {

	private float startState;

	// active slots after adjustment in the new state;
	// active slots must been running (w/o slow-start)
	private int numOfActiveSlots;

	// some running slots may not be active
	private int numOfRunningSlots;

	private float progressOfRunningSlots;

	// estimated remaining waves based on current jobState
	private float expectedTrackWaves;

	// estimated remaining time based on current jobState
	private float expectedTrackTime;

	public float getStartState() {
		return startState;
	}

	public void setStartState(float startState) {
		this.startState = startState;
	}

	public int getNumOfActiveSlots() {
		return numOfActiveSlots;
	}

	public void setNumOfActiveSlots(int numOfActiveSlots) {
		this.numOfActiveSlots = numOfActiveSlots;
	}

	public int getNumOfRunningSlots() {
		return numOfRunningSlots;
	}

	public void setNumOfRunningSlots(int numOfRunningSlots) {
		this.numOfRunningSlots = numOfRunningSlots;
	}

	public float getProgressOfRunningSlots() {
		return progressOfRunningSlots;
	}

	public void setProgressOfRunningSlots(float progressOfRunningSlots) {
		this.progressOfRunningSlots = progressOfRunningSlots;
	}

	public float getExpectedTrackWaves() {
		return expectedTrackWaves;
	}

	public void setExpectedTrackWaves(float expectedTrackWaves) {
		this.expectedTrackWaves = expectedTrackWaves;
	}

	public float getExpectedTrackTime() {
		return expectedTrackTime;
	}

	public void setExpectedTrackTime(float expectedTrackTime) {
		this.expectedTrackTime = expectedTrackTime;
	}

}
