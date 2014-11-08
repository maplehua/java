package com.ruc.xx427.log;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobHistoryCollectorTest {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "read log");
		String fileInPath = "hdfs://10.77.50.173:9000/tmp/hadoop-yarn/staging/history/done/2014/10/31/000000/job_1411033758207_0007-1414759852502-root-TeraSort-1414760272311-384-200-SUCCEEDED-default-1414759904260.jhist";
	    String fileOutPath = "I:\\hadoop.txt";
		FileInputFormat.addInputPath(job, new Path(fileInPath));
		FileOutputFormat.setOutputPath(job, new Path(fileOutPath));
		ArrayList<String> list = new ArrayList<String>();
		JobHistoryCollector.listFileNames("hdfs://10.77.50.173:9000/tmp/hadoop-yarn/staging/history/done/", conf, list);
		for (int i = 0; i < list.size(); i++) {
			String log = JobHistoryCollector.getLog(list.get(i), conf);
			System.out.println(log);
		}

	}

}
