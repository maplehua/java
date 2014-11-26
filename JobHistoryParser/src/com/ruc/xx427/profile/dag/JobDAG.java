package com.ruc.xx427.profile.dag;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import com.ruc.xx427.property.Property;


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

	public HashMap<JobNode, List<JobNode>> getChildrenGraph() {
		return childrenGraph;
	}
	
	public HashMap<JobNode, List<JobNode>> getParantsGraph() {
		return parantsGraph;
	}

	public JobDAG(String dagPath){		
		this.readFromFile(dagPath);
	}
	
	public JobNode getRoot(){
		return jobRoot;
	}
	
	private void setRoot(JobNode jobRoot) {
		this.jobRoot = jobRoot;
	}
	
	public List<JobNode> getChilds(JobNode node){
		return this.childrenGraph.get(node.getJobName());
	}
	
	public List<JobNode> getParants(JobNode node){
		return this.parantsGraph.get(node.getJobName());
	}
	
	
	/**
	 * for now : test ,assume the file format like 1 2 3 4,which 1 represents the ID and follow numbers
	 * represents the ID which has the line from 1 to. If one ID does't has the line , followed by #
	 * format : jobname 1 2 3 4 \n inputPath
	 * @param dagPath
	 * @return 
	 * @throws IOException 
	 * 
	 */
	private void readFromFile(String dagPath) {	
		childrenGraph = new HashMap<JobNode, List<JobNode>> ();

		//modified by @Xiaohua
		
		//First, get all the jobNodeInfo into a map
		try {
			JobNodeInfo.setJobNodeInfo(Property.jobNodeInfoFile);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		HashMap<String, JobNode> jobNodeInfo = JobNodeInfo.getJobNodeInfo();
		
		//Then, get the dag from a file
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(dagPath))));
			String jobNameList = "";
			boolean isRoot = true;
			boolean isEnd = false;
			while ((jobNameList = br.readLine()) != null) {
				StringTokenizer st = new StringTokenizer(jobNameList, " ");
				String jobName = st.nextToken().trim();
				String tmpJob = "";
				List<JobNode> childNodeList = new ArrayList<JobNode>();
				while (st.hasMoreTokens()) {
					tmpJob = st.nextToken().trim();
					if (!tmpJob.startsWith("#")) {
						childNodeList.add(jobNodeInfo.get(tmpJob));
					} else {
						isEnd = true;
					}
				}
				if (!isEnd) {
					this.childrenGraph.put(jobNodeInfo.get(jobName), childNodeList);
				}
				
				if (isRoot) {
					this.setRoot(jobNodeInfo.get(jobName));
					isRoot = false;
				}
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		//Last, generate the fatherList
		this.parantsGraph = new HashMap<JobNode, List<JobNode>>();
		HashMap<String, ArrayList<String>> childrenList = new HashMap<String, ArrayList<String>>();
		for (JobNode keyNode : childrenGraph.keySet()) {
			String childName = keyNode.getJobName();
			ArrayList<String> tmpChildList = new ArrayList<String>();
			for (JobNode listNode : childrenGraph.get(keyNode)) {
				tmpChildList.add(listNode.getJobName());
			}
			childrenList.put(childName, tmpChildList);
		}
		
		for (String jobName : childrenList.keySet()) {
			ArrayList<JobNode> tmpFatherList = new ArrayList<JobNode>();
			for (String tmpName : childrenList.keySet()) {
				ArrayList<String> list = childrenList.get(tmpName);
				if (list.contains(jobName)) {
					tmpFatherList.add(jobNodeInfo.get(tmpName));
				}
			}
			//root has no parent
			if (!tmpFatherList.isEmpty()) {
				this.parantsGraph.put(jobNodeInfo.get(jobName), tmpFatherList);
			}	
		}
		
	}
}
