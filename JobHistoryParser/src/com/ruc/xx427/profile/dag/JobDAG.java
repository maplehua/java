package com.ruc.xx427.profile.dag;

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
	private HashMap<JobNode, List<JobNode>> childrenGraph;
	private HashMap<JobNode, List<JobNode>> parantsGraph;

	public JobDAG(String dagPath){		
		this.readFromFile(dagPath);
	}
	
	public JobNode getRoot(){
		return jobRoot;
	}
	
	public List<JobNode> getChilds(JobNode node){
		return this.childrenGraph.get(node.getJobName());
	}
	
	public List<JobNode> getParants(JobNode node){
		return this.parantsGraph.get(node.getJobName());
	}
	
	
	
	private void readFromFile(String dagPath){	
		childrenGraph = new HashMap<JobNode, List<JobNode>> ();
		//TODO @xiaohua read the DAG (jobRoot and jobGraph) from the file with pre-defeind format
		
	}
}
