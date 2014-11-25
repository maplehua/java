package com.ruc.xx427.job.log;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.ruc.xx427.property.Property;

public class JobHistoryCollectorTest {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "read log");
		String fileInPath = Property.fileInputPath;
	    String fileOutPath = Property.fileOutputPath;
		FileInputFormat.addInputPath(job, new Path(fileInPath));
		FileOutputFormat.setOutputPath(job, new Path(fileOutPath));
		ArrayList<String> list = new ArrayList<String>();
		JobHistoryCollector.listFileNames(Property.historyLogDirectory, conf, list);
		for (int i = 0; i < list.size(); i++) {
			String log = JobHistoryCollector.getLog(list.get(i), conf);
			System.out.println(log);
		}

	}

}
