package com.ruc.xx427.optimizer;

import java.util.HashMap;
import java.util.Map;

import com.ruc.xx437.profile.dag.JobNode;

public class FlowFactors {

	private Map<JobNode, JobFactors> flowFactors;

	public FlowFactors() {
		this.flowFactors = new HashMap<JobNode, JobFactors>();
	}

	public void addJobConfig(JobNode node, JobFactors factors) {
		this.flowFactors.put(node, factors);
	}

	public JobFactors getJobConfig(JobNode node, JobFactors factors) {
		return this.flowFactors.get(node);
	}

}
