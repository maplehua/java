package com.ruc.xx427.parser;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.ruc.xx427.entity.FileSystemCounters;
import com.ruc.xx427.entity.JobHistoryRecord;
import com.ruc.xx427.entity.MapReduceFramework;

/*
 * Class  : JobHistoryParser
 * Author : xiaohua
 * Time   : 2014/11/6 10:26
 * Use    : parser the history log ; input : json string
 */
public class JobHistoryParser {

	/*
	 * @param : String log, in the history log, hdfs contains one line for every job,
	 *          which has all the information about the job
	 * @return : void
	 */
	public static JobHistoryRecord parseLog(String log) {
		try {
			JobHistoryRecord jobHistRecord = new JobHistoryRecord();
			JSONObject all = new JSONObject(log);
			JSONObject event = all.getJSONObject("event");
			JSONObject jobFinished = event.getJSONObject("org.apache.hadoop.mapreduce.jobhistory.JobFinished");
			JSONObject totalCounters = jobFinished.getJSONObject("totalCounters");
			JSONObject mapCounters = jobFinished.getJSONObject("mapCounters");
			JSONObject reduceCounters = jobFinished.getJSONObject("reduceCounters");
			parseCounters(jobHistRecord, totalCounters, JobHistoryRecord.TOTAL);
			parseCounters(jobHistRecord, mapCounters, JobHistoryRecord.MAP);
			parseCounters(jobHistRecord, reduceCounters, JobHistoryRecord.REDUCE);
		
			return jobHistRecord;
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return null;
	}
	private static void parseCounters(JobHistoryRecord jobHistRecord, JSONObject counters, int counterType) {
		try {
			JSONArray groups = counters.getJSONArray("groups");
			JSONObject fileSystemCounters = groups.getJSONObject(0);//file system counters
			JSONObject mapReduceFramework = new JSONObject();
			//different kind of counter, has different index, be careful
			if (counterType == JobHistoryRecord.TOTAL) {
				mapReduceFramework = groups.getJSONObject(2);//Map-Reduce Framework
			} else {
				mapReduceFramework = groups.getJSONObject(1);
			}
			parseFileSystemCounters(jobHistRecord, fileSystemCounters, counterType);
			parseMapReduceFramework(jobHistRecord, mapReduceFramework, counterType);
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
	
	private static void parseFileSystemCounters(JobHistoryRecord jobHistRecord, JSONObject fileSystemCounters, int counterType) {
		try {
			JSONArray counters = fileSystemCounters.getJSONArray("counts");
			for (int i = 0; i< FileSystemCounters.COUNT; i++) {
				JSONObject tmp = counters.getJSONObject(i);
				String displayName = tmp.getString("displayName");
				long value = Long.parseLong(tmp.getString("value"));
				jobHistRecord.setFileSystemCounters(displayName, counterType, value);
			}
			
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
	private static void parseMapReduceFramework(JobHistoryRecord jobHistRecord, JSONObject mapReduceFramework, int counterType) {
		try {
			JSONArray counters = mapReduceFramework.getJSONArray("counts");
			//System.out.println(counterType+"-------------"+counters.toString());
			for (int i = 0; i< MapReduceFramework.COUNT; i++) {
				JSONObject tmp = counters.getJSONObject(i);
				String displayName = tmp.getString("displayName");
				long value = Long.parseLong(tmp.getString("value"));
				jobHistRecord.setMapReduceFramework(displayName, counterType, value);
			}
			
		} catch (JSONException e) {
			//e.printStackTrace();
			//there is some exception because some value does't contain
			//for example: in map_reduceFramweork, reduce input maps : don't have map information
		}
	}
}
