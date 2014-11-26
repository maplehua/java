package com.ruc.xx427.property;
/**
 * 
 * Description: set all the static variable 
 *
 * @author Xiaohua
 * @date 2014-11-25
 *
 */
public class Property {

	// jobMeta file in JobHistoryRecord.java
	public static final String JobMetaFile = "I:\\jobMeta";
	public static final String JobCounterFile = "I:\\jobCounter";
	
	//HDFS file path
	public static final String fileInputPath = "hdfs://10.77.50.165:9000/tmp/hadoop-yarn/staging/history/done/2014/11/21/000000/job_1416570891698_0002-1416571012406-xx427-TeraSort-1416571066871-192-200-SUCCEEDED-root.xx427-1416571013636.jhist";
	public static final String fileOutputPath = "I:\\hadoop.txt";
	public static final String historyLogDirectory = "hdfs://10.77.50.165:9000/tmp/hadoop-yarn/staging/history/done/";
	
	//JobNode file path
	public static final String jobNodeInfoFile = "I:\\JobInfo.txt";
}
