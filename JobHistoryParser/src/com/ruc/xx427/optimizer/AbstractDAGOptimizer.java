package com.ruc.xx427.optimizer;

import java.util.Map;

import com.ruc.xx427.profile.dag.JobNode;

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
			jobQueue.get(jobNode).setDiskShare(0);
			jobQueue.get(jobNode).setNetShare(0);
			jobQueue.get(jobNode).setShareRate(0);

		}
	}

	/**
	 * estimate the transition time of each state based on lemma #2
	 * 
	 * @param jobQueue
	 * @return the job with minimum transaction state (i.e. the job will firstly
	 *         enter the next stage)
	 */
	protected JobNode estimateTransTime(Map<JobNode, JobState> jobQueue) {
		JobNode endStateJobNode = null;
		float minTransTime = Float.MAX_VALUE;
		for (JobNode jobNode : jobQueue.keySet()) {
			float transTime = 0;
			// TODO (@juwei) estimate the running time of each transition based
			// on lemma #2
			jobQueue.get(jobNode).setTransTime(transTime);

			// set the minimum transition time
			if (transTime < minTransTime) {
				endStateJobNode = jobNode;
			}
		}
		return endStateJobNode;
	}

	/**
	 * 
	 * For each job that is not the job will firstly end the stage, update the
	 * progress of its current stage based (lemma #3)
	 * 
	 * @param endNode
	 *            the job that will firstly end the current stage
	 * @param jobQueue
	 */
	protected void updateProgress(JobNode endNode,
			Map<JobNode, JobState> jobQueue) {
		float minTransTime = jobQueue.get(endNode).getTransTime();
		for (JobNode jobNode : jobQueue.keySet()) {
			float progress = jobQueue.get(jobNode).getProgress();
			// XXX (@juwei) re-think if it is correct
			progress += (1 - progress) * minTransTime
					/ jobQueue.get(jobNode).getTransTime();
			jobQueue.get(jobNode).setProgress(progress);
		}

	}

}
