package com.ruc.xx427.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Scanner;

import com.ruc.xx427.entity.JobHistoryRecord;
import com.ruc.xx427.invertedIndex.InvertedIndex;

/*
 * Class  : SearchJobHistory
 * Author : xiaohua
 * Time   : 2014/11/7 14:24
 * Use    : search for history
 */
public class SearchJobHistory {

	/*
	 * @param  : jobName , search the history by the jobname and find the one the most beside the size
	 * @param  : size , jobSize
	 * @return : void
	 */
	public static void searchHistory(Map<String, ArrayList<Long>> index, String jobName, long size) {
		try {
			if (index.containsKey(jobName)) {
				ArrayList<Long> value = index.get(jobName);
				for (int i = 0 ; i < value.size(); i++) {
					System.out.println(value.get(i));
					JobHistoryRecord jobTmp = JobHistoryRecord.read(value.get(i));
					jobTmp.getFileSystemCounters().printFileSystemCounters();
					System.out.println("----------------------------");
				}
			} else {
				System.out.println("Not Found");
			}
		} catch(IOException e) {
			e.printStackTrace();
		}
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
			searchHistory(index, jobName, jobSize);
		}
	}

}
