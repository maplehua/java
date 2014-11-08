package com.ruc.xx427.entity;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class JobHistoryRecordTest {

	public static void main(String[] args) {
		JobHistoryRecord.init();
		JobHistoryRecord jobTest = new JobHistoryRecord();
		File f = new File("I:\\jobtest.txt");
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
			jobTest.setJobID("job_1411033758207_0008");
			jobTest.setJobName("wordcount");
			jobTest.setJobSize(1000);
			for (int i = 0;i<10; i++) {//10 records in file system counters
				String tmp1 = br.readLine().trim();
				String tmp2 = br.readLine().trim();
				StringTokenizer st = new StringTokenizer(tmp2);
				long map = Long.parseLong(st.nextToken());
				long reduce = Long.parseLong(st.nextToken());
				long total = Long.parseLong(st.nextToken());
				//System.out.println(tmp1);
				//System.out.println(map+" "+reduce+" "+total);
				jobTest.setFileSystemCounters(tmp1,JobHistoryRecord.MAP , map);
				jobTest.setFileSystemCounters(tmp1,JobHistoryRecord.REDUCE , reduce);
				jobTest.setFileSystemCounters(tmp1,JobHistoryRecord.TOTAL , total);
			}
			for (int i = 0; i<20; i++) {
				String tmp1 = br.readLine().trim();
				String tmp2 = br.readLine().trim();
				StringTokenizer st = new StringTokenizer(tmp2);
				long map = Long.parseLong(st.nextToken());
				long reduce = Long.parseLong(st.nextToken());
				long total = Long.parseLong(st.nextToken());
				jobTest.setMapReduceFramework(tmp1, JobHistoryRecord.MAP, map);
				jobTest.setMapReduceFramework(tmp1, JobHistoryRecord.REDUCE, reduce);
				jobTest.setMapReduceFramework(tmp1, JobHistoryRecord.TOTAL, total);
			}
			JobHistoryRecord.write(jobTest);
			JobHistoryRecord t = new JobHistoryRecord();
			t = JobHistoryRecord.read(0);
			System.out.println(t.getJobName());
			System.out.println(t.getJobSize());
			System.out.println(t.getJobID());
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		JobHistoryRecord.close();
	}

}
