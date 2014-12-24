package com.ruc.xx427.optimizer.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.mapred.JobConf;

import com.ruc.xx427.optimizer.AbstractDAGOptimizer;
import com.ruc.xx427.optimizer.model.FlowFactors;
import com.ruc.xx427.optimizer.model.JobStage;
import com.ruc.xx427.optimizer.model.JobState;
import com.ruc.xx427.profile.YarnDAGProfiles;
import com.ruc.xx427.profile.dag.JobNode;

/**
 * The Brute-force search based search to find the optimal configuration of jobs
 * in DAG TODO summarize the transition time to evaluate the accuracy of the
 * cost model
 * 
 * @author Juwei
 * 
 */
public class BruteForceOptimizer extends AbstractDAGOptimizer {

	@Override
	public Map<String, JobConf> optimize(String dagPath) {

		Set<FlowFactors> flowFactorsSet = new HashSet<FlowFactors>();
		// TODO @juwei generate the candidates
		float minCost = Float.MAX_VALUE;
		float curCost = 0;
		FlowFactors bestFlowFactors = null;
		for (FlowFactors flowFactors : flowFactorsSet) {
			YarnDAGProfiles dagProfiles = new YarnDAGProfiles(dagPath);
			curCost = this.estimateCost(dagProfiles, flowFactors);
			if (curCost < minCost) {
				minCost = curCost;
				bestFlowFactors = flowFactors;
			}

		}
		return this.fromFactorsToConfigs(bestFlowFactors);
	}

	/**
	 * 
	 * @param dagProfiles
	 * @return the estimated time cost (//TODO add the trace of state transition
	 *         for test @xiaohua)
	 */
	public float estimateCost(YarnDAGProfiles dagProfiles,
			FlowFactors flowFactors) {

		Map<JobNode, JobState> jobQueue = new HashMap<JobNode, JobState>();
		float flowTime = 0;
		// add the root job
		JobState rootState = null; // TODO new JobSate
		// XXX the job state will be updated later

		jobQueue.put(dagProfiles.getJobDAG().getRoot(), rootState);

		while (jobQueue.size() > 0) {
			boolean jobEndFlag = false;
			JobNode endJSNode = null;
			while (!jobEndFlag) {
				this.updateShareStates(jobQueue);
				endJSNode = this.estimateTransTime(jobQueue, flowFactors);
				// we can get transition time of this node from jobQueue, so
				// there is not need to explicitly get the transition time.
				this.updateProgress(endJSNode, jobQueue);
				flowTime += jobQueue.get(endJSNode).getTransTime();

				// update the stage of endNode
				JobStage stage = jobQueue.get(endJSNode).getJobStage();
				if ((stage == JobStage.REDUCE) || endJSNode.isMapOnly()) {
					jobEndFlag = true;
					jobQueue.remove(endJSNode);
				} else if (stage == JobStage.MAP) {
					jobQueue.get(endJSNode).setJobStage(JobStage.HYBRID);
				} else {
					// XXX do we need to consider the case that slow-start=1
					// thus no hybrid? It can be covered by hybrid stage time
					// estimation (=0)
					jobQueue.get(endJSNode).setJobStage(JobStage.REDUCE);
				}
			}
			// select next jobs from DAG
			for (JobNode childNode : dagProfiles.getJobDAG().getChilds(
					endJSNode)) {
				boolean canStart = true;
				// XXX replace this for with set operations like isContainAll(),
				// etc.
				for (JobNode parantNode : dagProfiles.getJobDAG().getParants(
						childNode)) {
					if (jobQueue.keySet().contains(parantNode)) {
						canStart = false;
						break;
					}
				}
				if (canStart) {
					jobQueue.put(childNode, null);
					// TODO new JobState(JobStage.MAP, 0, 0, null)
					// the jobState will be updated later
				}
			}

		}
		return flowTime;
	}

	/**
	 * 
	 * @param factors
	 * @return
	 */
	private Map<String, JobConf> fromFactorsToConfigs(FlowFactors factors) {

		Map<String, JobConf> jobConfMap = new HashMap<String, JobConf>();

		// TODO @juwei from factors to configurations
		return jobConfMap;

	}

}
