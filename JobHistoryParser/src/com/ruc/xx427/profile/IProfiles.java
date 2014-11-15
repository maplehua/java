package com.ruc.xx427.profile;

import com.ruc.xx427.entity.JobHistoryRecord;
import com.ruc.xx427.profile.dag.JobDAG;
import com.ruc.xx427.profile.data.DataProfile;
import com.ruc.xx427.profile.rsc.ResourceProfile;
/**
 * 
 * Description: the interface of querying (so it does not include the generation interface of 
 * the profiles) profiles (job, data, resource, DAG)
 *
 * @author Juwei
 * @date 2014-11-14
 *
 */
public interface IProfiles {
    /**
     * 
     * Description: 
     *
     * @return
     */
	JobDAG getJobDAG();

	/**
	 * 
	 * Description: 
	 *
	 * @param jobName
	 * @param inputPath
	 * @return
	 */
	JobHistoryRecord findJobHistoryProfile(String jobName, String inputPath);
	
	/**
	 * 
	 * Description: 
	 *
	 * @param inputPath
	 * @return
	 */
	DataProfile getDataProfile(String inputPath);
	
	/**
	 * 
	 * Description: 
	 *
	 * @return
	 */
	ResourceProfile getRscProfile();
	

}
