package com.ruc.xx427.profile.dag;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.StringTokenizer;

import com.ruc.xx427.property.Property;

/*
 * read from jobNodeInfo file set all the information into a map
 * @author: Xiaohua
 * @time: 2014/11/25 21:18
 */
public class JobNodeInfo {

	private static HashMap<String, JobNode> jobNodeInfo;
	
	public static HashMap<String, JobNode> getJobNodeInfo() {
		return jobNodeInfo;
	}
	public static void setJobNodeInfo(String jobNodeInfoFilePath) throws IOException {
		
		jobNodeInfo = new HashMap<String, JobNode>();
		
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(jobNodeInfoFilePath))));
		
		String infoLine = "";
		while ((infoLine = br.readLine()) != null) {
			StringTokenizer jobNodeTokenizer = new StringTokenizer(infoLine, " ");
			String jobName = jobNodeTokenizer.nextToken().trim();
			String inputPath = jobNodeTokenizer.nextToken().trim();
			boolean isMapOnly = jobNodeTokenizer.nextToken().trim().startsWith("false") ? false :true;
			JobNode tmpNode = new JobNode();
			tmpNode.setInputPath(inputPath);
			tmpNode.setJobName(jobName);
			tmpNode.setMapOnly(isMapOnly);
			jobNodeInfo.put(jobName, tmpNode);
		}
		br.close();
	}
}
