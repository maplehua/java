package com.ruc.xx427.profile;

import java.util.ArrayList;
import java.util.Map;

import com.ruc.xx427.job.entity.JobHistoryRecord;
import com.ruc.xx427.job.invertedIndex.InvertedIndex;
import com.ruc.xx427.job.search.SearchJobHistory;
import com.ruc.xx427.profile.dag.JobDAG;
import com.ruc.xx427.profile.data.DataProfile;
import com.ruc.xx427.profile.rsc.ResourceProfile;

public class YarnDAGProfiles implements IProfiles{
	
	private JobDAG jobDAG;
	
	private Map<String, ArrayList<Long>> jobIndex; 
	
	public YarnDAGProfiles(String dagPath){
		this.jobDAG = new JobDAG(dagPath);
		this.jobIndex = InvertedIndex.createIndex(); //TODO @xiaohua add the init of index from config files
	}

	@Override
	public JobDAG getJobDAG() {
		return this.jobDAG;
	}

	@Override
	public JobHistoryRecord findJobHistoryProfile(String jobName,
			String inputPath) {
		//inputPath -> get the job size -> search(jobname, size)
		//how to read the size according to the job input path
		DataProfile dataProfile = new DataProfile();
		dataProfile = dataProfile.setDataInfo(inputPath);
		long jobSize = dataProfile.getDataSize();	
		return SearchJobHistory.searchHistory(jobIndex, jobName, jobSize);
	}

	@Override
	public DataProfile getDataProfile(String inputPath) {
		//add by xiaohua at 2014/11/29
		DataProfile dataProfile = new DataProfile();
		return dataProfile.setDataInfo(inputPath);
	}

	@Override
	public ResourceProfile getRscProfile() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
