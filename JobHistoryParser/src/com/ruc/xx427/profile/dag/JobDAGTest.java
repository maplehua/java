package com.ruc.xx427.profile.dag;

public class JobDAGTest {

	public static void main(String[] args) {
		JobDAG dag= new JobDAG("I:\\DAGInfo.txt");
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

	}

}
