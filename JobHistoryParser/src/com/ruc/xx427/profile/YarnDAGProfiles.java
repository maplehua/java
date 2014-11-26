package com.ruc.xx427.profile;

import java.util.ArrayList;
import java.util.Map;

import com.ruc.xx427.job.entity.JobHistoryRecord;
import com.ruc.xx427.profile.dag.JobDAG;
import com.ruc.xx427.profile.data.DataProfile;
import com.ruc.xx427.profile.rsc.ResourceProfile;

public class YarnDAGProfiles implements IProfiles{
	
	private JobDAG jobDAG;
	
	private Map<String, ArrayList<Long>> jobIndex; 
	
	public YarnDAGProfiles(String dagPath){
		this.jobDAG = new JobDAG(dagPath);
		this.jobIndex = null; //TODO @xiaohua add the init of index from config files
	}

	@Override
	public JobDAG getJobDAG() {
		return this.jobDAG;
	}

	@Override
	public JobHistoryRecord findJobHistoryProfile(String jobName,
			String inputPath) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataProfile getDataProfile(String inputPath) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResourceProfile getRscProfile() {
		// TODO Auto-generated method stub
		return null;
	}
	

	

}
