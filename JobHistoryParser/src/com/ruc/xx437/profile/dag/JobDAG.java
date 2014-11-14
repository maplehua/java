package com.ruc.xx437.profile.dag;

import java.util.HashMap;
import java.util.List;


/**
 * 
 * Description: 
 *
 * @author Juwei
 * @date 2014-11-14
 *
 */
public class JobDAG {
	
	private JobNode jobRoot;
	private HashMap<JobNode, List<JobNode>> jobGraph;

	public JobDAG(String dagPath){		
		this.readFromFile(dagPath);
	}
	
	public JobNode getRoot(){
		return jobRoot;
	}
	
	public List<JobNode> getChilds(JobNode node){
		return this.jobGraph.get(node.getJobName());
	}
	
	private void readFromFile(String dagPath){	
		jobGraph = new HashMap<JobNode, List<JobNode>> ();
		//TODO @xiaohua read the DAG (jobRoot and jobGraph) from the file with pre-defeind format
		
	}
}
