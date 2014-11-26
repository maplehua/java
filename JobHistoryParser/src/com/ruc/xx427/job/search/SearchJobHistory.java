package com.ruc.xx427.job.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Scanner;

import com.ruc.xx427.job.entity.JobHistoryRecord;
import com.ruc.xx427.job.invertedIndex.InvertedIndex;

/*
 * Class  : SearchJobHistory
 * Author : xiaohua
 * Time   : 2014/11/7 14:24
 * Use    : search for history
 */
public class SearchJobHistory {

	private static final long MAX  =2000000000; 
	
	/*
	 * @param  : jobName , search the history by the jobname and find the one the most beside the size
	 * @param  : size , jobSize
	 * @return : JobHistoryRecord, if failed, return null
	 */
	public static JobHistoryRecord searchHistory(Map<String, ArrayList<Long>> index, String jobName, long size) {
		try {
			if (index.containsKey(jobName)) {
				long deta = SearchJobHistory.MAX;
				JobHistoryRecord tmpValue = new JobHistoryRecord();;// to store the tmp result
				ArrayList<Long> value = index.get(jobName);
				for (int i = 0 ; i < value.size(); i++) {
					JobHistoryRecord jobTmp = JobHistoryRecord.read(value.get(i));
					if (jobTmp.getJobSize() - size < deta) {
						deta = jobTmp.getJobSize() - size;
						tmpValue = jobTmp;
					}	
				}
				return tmpValue;
			} 
		} catch(IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	public static void main(String[] args) {
		Scanner scan = new Scanner(System.in);
		JobHistoryRecord.init();
		Map<String, ArrayList<Long>> index = InvertedIndex.createIndex();
		System.out.println(index.keySet().toString());
		while (true) {
			System.out.print("Please input the jobName and jobSize :  ");
			String jobName = scan.next();
			long jobSize = scan.nextLong();
			JobHistoryRecord tmp = searchHistory(index, jobName, jobSize);
			tmp.getFileSystemCounters().printFileSystemCounters();
			tmp.getMapReduceFramework().printMapReduceFramework();
			System.out.println(tmp.getJobSize());
		}
	}

}
