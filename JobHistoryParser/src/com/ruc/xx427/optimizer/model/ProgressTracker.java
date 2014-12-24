package com.ruc.xx427.optimizer.model;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * 
 * Description: because the progress deptends on the state transistion
 * 
 * @author Juwei
 * @date 2014Äê12ÔÂ23ÈÕ
 * 
 */
public class ProgressTracker {

	private int totalMapTask;

	private int totalReduceTask;

	private int completedMapTask;

	private int completedReduceTask;

	private Set<AllocatedSlots> runningMapSlotTracks;

	private Set<AllocatedSlots> runningReduceSlotTracks;

	public ProgressTracker() {
		this.runningMapSlotTracks = new TreeSet<AllocatedSlots>(
				new AllocatedSlotsComparator());
		this.runningReduceSlotTracks = new TreeSet<AllocatedSlots>(
				new AllocatedSlotsComparator());
	}

	public int getTotalMapTask() {
		return totalMapTask;
	}

	public void setTotalMapTask(int totalMapTask) {
		this.totalMapTask = totalMapTask;
	}

	public int getTotalReduceTask() {
		return totalReduceTask;
	}

	public void setTotalReduceTask(int totalReduceTask) {
		this.totalReduceTask = totalReduceTask;
	}

	public int getCompletedMapTask() {
		return completedMapTask;
	}

	public void setCompletedMapTask(int completedMapTask) {
		this.completedMapTask = completedMapTask;
	}

	public int getCompletedReduceTask() {
		return completedReduceTask;
	}

	public void setCompletedReduceTask(int completedReduceTask) {
		this.completedReduceTask = completedReduceTask;
	}

	public Set<AllocatedSlots> getRunningMapSlotTracks() {
		return runningMapSlotTracks;
	}

	public Set<AllocatedSlots> getRunningReduceSlotTracks() {
		return runningReduceSlotTracks;
	}

	

}
