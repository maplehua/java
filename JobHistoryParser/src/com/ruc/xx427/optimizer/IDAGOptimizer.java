

package com.ruc.xx427.optimizer;

import java.util.Map;
import org.apache.hadoop.mapred.JobConf;

/**
 * 
 * Description: the interface to generate all the configuration for a given DAG based job work-flow. 
 *
 * @author Juwei
 * @date 2014-11-14
 *
 */
public interface IDAGOptimizer {
	
    
	public Map<String, JobConf> optimize(String dagPath); //XXX change JobConf to generic if we want to support Spark


}
