package com.ruc.xx437.profile.dag;
/**
 * 
 * Description: a JobNode is a node in the DAG that defines the work-flow. (e.g. A MapReduce job) 
 *
 * @author Juwei
 * @date 2014-11-14
 *
 */
public class JobNode {
	
	private String jobName;
	
	private String inputPath;

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
	
	

}
