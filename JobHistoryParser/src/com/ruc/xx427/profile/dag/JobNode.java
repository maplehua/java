package com.ruc.xx427.profile.dag;

/**
 * 
 * Description: a JobNode is a node in the DAG that defines the work-flow. (e.g.
 * A MapReduce job)
 * 
 * @author Juwei
 * @date 2014-11-14
 * 
 */
public class JobNode {

	private String jobName;
	private String inputPath;
	// XXX we may need to retrieve the map only job information from the
	// profile, then we need to set this flag based on the flag at some time,
	// but it is OK to keep this flag being manually enabled in paper
	// experiments.
	private boolean mapOnly;

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	public boolean isMapOnly() {
		return mapOnly;
	}

	public void setMapOnly(boolean mapOnly) {
		this.mapOnly = mapOnly;
	}

}
