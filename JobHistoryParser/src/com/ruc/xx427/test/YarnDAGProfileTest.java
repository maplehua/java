package com.ruc.xx427.test;

import com.ruc.xx427.job.entity.JobHistoryRecord;
import com.ruc.xx427.profile.YarnDAGProfiles;
import com.ruc.xx427.profile.dag.JobDAG;
import com.ruc.xx427.profile.dag.JobNode;
import com.ruc.xx427.profile.data.DataProfile;

public class YarnDAGProfileTest {

	public static void main(String[] args) {
		YarnDAGProfiles yarnDAGProfile = new YarnDAGProfiles("I:\\DAGInfo.txt");
		
		//part 1. test getJobDAG
		System.out.println("--------------------test getJobDAG---------");
		JobDAG dag= yarnDAGProfile.getJobDAG();
		System.out.println("Root name = "+dag.getRoot().getJobName());
		System.out.println("childrenGraph belows : ");
		for (JobNode jobNode : dag.getChildrenGraph().keySet()) {
			System.out.print(jobNode.getJobName()+"  :  ");
			for (JobNode tmp : dag.getChildrenGraph().get(jobNode)) {
				System.out.print(tmp.getJobName()+"  ");
			}
			System.out.println();
		}
		
		System.out.println("parentGraph belows : ");
		for (JobNode jobNode : dag.getParantsGraph().keySet()) {
			System.out.print(jobNode.getJobName()+"  :  ");
			for (JobNode tmp : dag.getParantsGraph().get(jobNode)) {
				System.out.print(tmp.getJobName()+"  ");
			}
			System.out.println();
		}
		
		//part 2. test findJobHistoryProfile
		System.out.println("--------------------test findJobHistoryProfile----------");
		JobHistoryRecord jobHist = yarnDAGProfile.findJobHistoryProfile("TeraSort", "hdfs://10.77.50.165:9000/user/xx427/terasort/10000000-input");
		jobHist.getFileSystemCounters().printFileSystemCounters();
		jobHist.getMapReduceFramework().printMapReduceFramework();
		
		//part 3. test getDataProfile
		System.out.println("--------------------test getDataProfile----------");
		DataProfile dataProfile = yarnDAGProfile.getDataProfile("hdfs://10.77.50.165:9000/user/xx427/terasort/10000000-input");
		System.out.println(dataProfile.getBlockSize());
		System.out.println(dataProfile.getDataSize());
		System.out.println(dataProfile.getNumOfFiles());
		System.out.println(dataProfile.getInputPath());
		
		
		


	}

}
