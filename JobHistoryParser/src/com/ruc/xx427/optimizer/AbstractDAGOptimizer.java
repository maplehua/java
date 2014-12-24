package com.ruc.xx427.optimizer;

import java.util.Map;
import java.util.Set;

import com.ruc.xx427.job.entity.JobHistoryRecord;
import com.ruc.xx427.optimizer.model.AllocatedRsc;
import com.ruc.xx427.optimizer.model.AllocatedSlots;
import com.ruc.xx427.optimizer.model.FlowFactors;
import com.ruc.xx427.optimizer.model.JobFactors;
import com.ruc.xx427.optimizer.model.JobStage;
import com.ruc.xx427.optimizer.model.JobState;
import com.ruc.xx427.optimizer.model.ProgressTracker;
import com.ruc.xx427.profile.dag.JobNode;
import com.ruc.xx427.profile.data.DataProfile;
import com.ruc.xx427.profile.rsc.ResourceProfile;

public abstract class AbstractDAGOptimizer implements IDAGOptimizer {
	/**
	 * for each job in the queue, estimate {diskShare, netShare, shareRate} and
	 * then update these shares in the share state (lemma #1)
	 * 
	 * @param jobQueue
	 */
	protected void updateShareStates(Map<JobNode, JobState> jobQueue) {
		for (JobNode jobNode : jobQueue.keySet()) {

			// TODO (@juwei) set the three shares based on lemma #1
			jobQueue.get(jobNode).setMapShare(new AllocatedRsc());

		}
	}

	/**
	 * estimate the transition time of each state based on lemma #2
	 * 
	 * @param jobQueue
	 * @return the job with minimum transaction state (i.e. the job will firstly
	 *         enter the next stage); the estimated remaining time of each track
	 */
	protected JobNode estimateTransTime(Map<JobNode, JobState> jobQueue,
			FlowFactors flowFactors) {
		JobNode endStateJobNode = null;
		float minTransTime = Float.MAX_VALUE;
		for (JobNode jobNode : jobQueue.keySet()) {

			float transTime = 0;

			// TODO (@juwei) estimate the running time of each transition based
			// on lemma #2

			JobHistoryRecord jobProfile = null; // TODO get the job profile
			JobFactors jobFactors = null; // TODO get the job factor
			DataProfile dataProfile = null; // TODO get the data profile
			ResourceProfile resourceProfile = null; // TODO get the rsc profile

			// get state related variables
			JobState jobState = jobQueue.get(jobNode);
			ProgressTracker progressTracker = jobState.getProgressTracker();

			if (jobState.getJobStage() == JobStage.MAP) {

				float mapTaskTime = this.estmiateMapTime(jobProfile,
						dataProfile, resourceProfile, jobFactors, jobState);
				int numOfTaskMapStage = (int) (((float) jobFactors.getMapNum()) * jobFactors
						.getSlowStart());
				transTime = this.estimateWavesOfEachRunningTrack(
						progressTracker.getRunningMapSlotTracks(),
						numOfTaskMapStage,
						progressTracker.getCompletedMapTask(), mapTaskTime);

			} else if (jobState.getJobStage() == JobStage.HYBRID) {
				// map time
				float mapTaskTime = this.estmiateMapTime(jobProfile,
						dataProfile, resourceProfile, jobFactors, jobState);
				int numOfTaskHybridStage = jobFactors.getMapNum()
						- (int) (((float) jobFactors.getMapNum()) * jobFactors
								.getSlowStart());
				transTime = this.estimateWavesOfEachRunningTrack(
						progressTracker.getRunningMapSlotTracks(),
						numOfTaskHybridStage,
						progressTracker.getCompletedMapTask(), mapTaskTime);
				// TODO check if the shuffle progress will also be updated based
				// on map time

			} else if (jobState.getJobStage() == JobStage.REDUCE) {

				float shuffleTaskTime = this.estmiateShuffleTime(jobProfile,
						dataProfile, resourceProfile, jobFactors, jobState);
				float reduceTaskTime = this.estmiateReduceTime(jobProfile,
						dataProfile, resourceProfile, jobFactors, jobState);

				float bottleneckReduceWave = this
						.estimateWavesOfEachRunningTrack(
								progressTracker.getRunningReduceSlotTracks(),
								jobFactors.getReduceNum(),
								progressTracker.getCompletedMapTask(),
								reduceTaskTime);
				float pendingWaves = (float) Math
						.floor((double) bottleneckReduceWave);
				float runningWave = bottleneckReduceWave - pendingWaves;

				if (runningWave > 0.5f) {
					// shuffle stage
					transTime = (1 - runningWave) * 2 * shuffleTaskTime
							+ (1 + pendingWaves) * reduceTaskTime;
				} else {
					// reduce stage
					transTime = pendingWaves * shuffleTaskTime
							+ bottleneckReduceWave * reduceTaskTime;
				}
			}

			jobQueue.get(jobNode).setTransTime(transTime);

			// set the minimum transition time
			if (transTime < minTransTime) {
				endStateJobNode = jobNode;
			}
		}
		return endStateJobNode;
	}

	// need for test and evaluation
	/**
	 * 
	 * Description: estimate the maximum wave of the bottleneck slot track; We
	 * consider the impact of waves, different capacity of slot tracks (added or
	 * removed), and different task start time of each track.
	 * 
	 * TODO we may need a figure in the paper to illustrate this method
	 * 
	 * @param setOfSlots
	 * @param numOfTasksInStage
	 * @param completedTasks
	 * @return bottleneckWaves, {expectedWaves, expectedTime} in each track (in
	 *         setOfSlots)
	 */
	private float estimateWavesOfEachRunningTrack(
			Set<AllocatedSlots> setOfSlots, int numOfTasksInStage,
			int completedTasks, float taskTime) {
		float bottleneckWaves = 0;
		float currentTrackWave = 0;
		// (a) calculate runningTasks
		int runningTasks = 0;
		int activeTotalSlots = 0;
		for (AllocatedSlots slots : setOfSlots) {
			runningTasks += slots.getNumOfRunningSlots();
			activeTotalSlots += slots.getNumOfActiveSlots();
		}
		int remainingTasks = numOfTasksInStage - runningTasks - completedTasks;
		// (b) calulate common waves if there is
		int commonWaves = remainingTasks / activeTotalSlots; // floor()
		// (c) find the bottleneck track from sorted slot tracks
		int numOfTasksInLastWave = remainingTasks - commonWaves
				* activeTotalSlots;
		if (numOfTasksInLastWave != 0) {
			// iterate the sorted list
			for (AllocatedSlots slots : setOfSlots) {
				// here we estimate the remaining waves without counting running
				// tasks
				// TODO test if we assume the new stated track has been assigned
				// the task as progress=0
				// TODO test the case when slots are reduced in the new stage
				// TODO test the case active slots < running slots
				if (slots.getNumOfActiveSlots() > 0) {
					if (numOfTasksInLastWave >= 0) {
						// common wave;
						// current remaining progress of the last wave's track;
						// last wave;
						currentTrackWave = commonWaves
								+ (1 - slots.getProgressOfRunningSlots()) + 1;
						bottleneckWaves = currentTrackWave; // sortedList
					} else {
						currentTrackWave = commonWaves
								+ (1 - slots.getProgressOfRunningSlots());
					}
					// assign the tasks to slot track
					slots.setExpectedTrackWaves(currentTrackWave);
					slots.setExpectedTrackTime(currentTrackWave * taskTime);
					numOfTasksInLastWave -= slots.getNumOfActiveSlots();
				} else {
					// just finish the current task and release the slot track
					currentTrackWave = 1 - slots.getProgressOfRunningSlots();
					slots.setExpectedTrackWaves(currentTrackWave);
					slots.setExpectedTrackTime(currentTrackWave * taskTime);
				}
			}
		} else {

			for (AllocatedSlots slots : setOfSlots) {
				if (slots.getNumOfActiveSlots() > 0) {
					// just finish the current task and release the slot track
					currentTrackWave = commonWaves
							+ (1 - slots.getProgressOfRunningSlots());
					slots.setExpectedTrackWaves(currentTrackWave);
					slots.setExpectedTrackTime(currentTrackWave * taskTime);
				} else {
					// just finish the current task and release the slot track
					currentTrackWave = 1 - slots.getProgressOfRunningSlots();
					slots.setExpectedTrackWaves(currentTrackWave);
					slots.setExpectedTrackTime(currentTrackWave * taskTime);
				}
			}
			// the last one in the sorted list is the slowest
			bottleneckWaves = currentTrackWave;
		}
		return bottleneckWaves * taskTime;
	}

	// need for test and evaluation
	/**
	 * 
	 * Description: the idea is to map task time oriented estimation
	 * 
	 * @param jobProfile
	 * @param dataProfile
	 * @param resourceProfile
	 * @param jobFactors
	 * @param jobState
	 * @return
	 */
	private float estmiateMapTime(JobHistoryRecord jobProfile,
			DataProfile dataProfile, ResourceProfile resourceProfile,
			JobFactors jobFactors, JobState jobState) {
		// (a) schedule time of the task
		float scheduleTime = resourceProfile.getScheduleTime();
		// (b) map function, sort and compression time of the task
		float mapInputSplitSize = dataProfile.getDataSize()
				/ jobFactors.getMapNum();
		float mapFunTime = jobProfile.getMapFunTime() * mapInputSplitSize;
		// (c) map output disk write time of the task,
		// TODO we assume materialized map output of the profile is compressed
		float mapOutputCompress = (jobProfile.getMapReduceFramework().map_output_mater[0] / jobProfile
				.getMapReduceFramework().input_split[0]) * mapInputSplitSize;
		float mapOutputMem = (jobProfile.getMapReduceFramework().map_output_mater[0] / jobProfile
				.getMapReduceFramework().input_split[0]) * mapInputSplitSize;
		float mapOutputMater;
		if (jobFactors.isMapCompress()) {
			mapOutputMater = mapOutputCompress;
		} else {
			mapOutputMater = mapOutputMem;
		}
		float mapOutputDiskTime = mapOutputMater
				* jobState.getMapShare().getDiskWritePerTaskTP(); // shareRate
		// (d) map input read time
		float mapInputDiskTime = mapInputSplitSize
				* jobState.getMapShare().getDiskReadPerTaskTP();
		float mapTime = (float) (scheduleTime + mapFunTime + mapOutputDiskTime + mapInputDiskTime);

		return mapTime;
	}

	private float estmiateShuffleTime(JobHistoryRecord jobProfile,
			DataProfile dataProfile, ResourceProfile resourceProfile,
			JobFactors jobFactors, JobState jobState) {

		float shuffleTaskSize = ((jobProfile.getMapReduceFramework().map_output_mater[0] / jobProfile
				.getMapReduceFramework().input_split[0]))
				* dataProfile.getDataSize() / jobFactors.getReduceNum();
		// TODO we only count network time here!
		float shuffleTime = (float) shuffleTaskSize
				* resourceProfile.getNetTP();
		return shuffleTime;
	}

	private float estmiateReduceTime(JobHistoryRecord jobProfile,
			DataProfile dataProfile, ResourceProfile resourceProfile,
			JobFactors jobFactors, JobState jobState) {

		float shuffleTaskSize = ((jobProfile.getMapReduceFramework().map_output_mater[0] / jobProfile
				.getMapReduceFramework().input_split[0]))
				* dataProfile.getDataSize() / jobFactors.getReduceNum();

		float reduceInputTime = shuffleTaskSize
				* resourceProfile.getDiskReadTP();
		// TODO we assume the profile is compressed and the new plan is
		// compressed also
		float reduceFunTime = shuffleTaskSize * jobProfile.getReduceFunTime();
		float reduceSel = jobProfile.getFileSystemCounters().hdfs_number_write[1]
				/ jobProfile.getFileSystemCounters().hdfs_number_read[0];
		float reduceOutputSize = dataProfile.getDataSize() * reduceSel;
		float reduceOutputTime = reduceOutputSize
				* Math.max(2 * shuffleTaskSize * resourceProfile.getNetTP(),
						shuffleTaskSize * 3 * resourceProfile.getDiskWriteTP());

		return (reduceInputTime + reduceFunTime + reduceOutputTime);
	}

	/**
	 * 
	 * For each slot track of running jobs, update the progress of its current
	 * stage based (lemma #3)
	 * 
	 * We assume shuffle 50% and reduce 50%
	 * 
	 * @param endNode
	 *            the job that will firstly end the current stage
	 * @param jobQueue
	 */
	protected void updateProgress(JobNode endNode,
			Map<JobNode, JobState> jobQueue) {
		float minTransTime = jobQueue.get(endNode).getTransTime();
		float progress;
		int completedTasks;
		for (JobNode jobNode : jobQueue.keySet()) {
			JobState jobState = jobQueue.get(jobNode);
			
			if (jobState.getJobStage() == JobStage.MAP) {
				for (AllocatedSlots slot : jobState.getProgressTracker()
						.getRunningMapSlotTracks()) {
					// TODO test if completedMapTasks update correctly
					progress = slot.getProgressOfRunningSlots();
					progress += slot.getExpectedTrackWaves() * minTransTime
							/ slot.getExpectedTrackTime();
					slot.setProgressOfRunningSlots(Math.min(progress, 1.0f));

					completedTasks = jobState.getProgressTracker()
							.getCompletedMapTask() + (int) Math.floor(progress);
					jobState.getProgressTracker().setCompletedMapTask(
							completedTasks);
				}
			}else if (jobState.getJobStage() == JobStage.HYBRID){
				//TODO 
			}else if (jobState.getJobStage() == JobStage.REDUCE){
				//TODO
			}

			
		}
		
		
		/*
		float progress = jobQueue.get(jobNode).getProgress();
		// XXX (@juwei) re-think if it is correct
		progress += (1 - progress) * minTransTime
				/ jobQueue.get(jobNode).getTransTime();
		jobQueue.get(jobNode).setProgress(progress);

		// shuffle time, reduce slots are add-only in the hybrid stage
		float shuffleTaskSize = ((jobProfile.getMapReduceFramework().map_output_mater[0] / jobProfile
				.getMapReduceFramework().input_split[0]))
				* dataProfile.getDataSize() / jobFactors.getReduceNum();
		float slowestProgress = 1.0f;
		for (AllocatedSlots reduceSlots : progressTracker
				.getRunningReduceSlotTracks()) {
			if (slowestProgress < reduceSlots.getProgressOfRunningSlots())
				slowestProgress = reduceSlots.getProgressOfRunningSlots();
		}
		float tp = jobState.getReduceShare().getDiskReadPerTaskTP()
				+ jobState.getReduceShare().getDiskWritePerTaskTP()
				+ jobState.getReduceShare().getNetPerTaskTP();
		float shuffleTime = shuffleTaskSize * (1 - slowestProgress) * tp;
		// XXX (a) we ignore the last wave shuffle time (b) assume that disk/net
		// share considers various overlaps when it is updated
		 */
	}

}
