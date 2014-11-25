package com.ruc.xx427.job.log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/*
 * Class  : JobHistoryCollector
 * Author : xiaohua
 * Time   : 2014/11/7 9:52
 * Use    : pull the jobHistory records from remote HDFS, here we only use the file *.jhist
 */
public class JobHistoryCollector {

	/*
	 * @param : fileInputPath, HDFS address, like hdfs://10.77.50.173:9000/tmp/hadoop-yarn/staging/history/done/2014/10/31/000000/job_1411033758207_0007-
	 * 		    +1414759852502-root-TeraSort-1414760272311-384-200-SUCCEEDED-default-1414759904260.jhist
	 * @param : job Configuration
	 * @return : String line ,which contains the log (the last line in the *.jhist)
	 */
	public static String getLog(String fileInputPath, Configuration conf) {
		try {
			FileSystem fs_in;
			fs_in = FileSystem.get(new URI(fileInputPath),conf);
			FSDataInputStream fsInputStream = fs_in.open(new Path(fileInputPath));
			BufferedReader br = new BufferedReader(new InputStreamReader(fsInputStream));
			String line = null;
			while ((line = br.readLine()) != null) {
				if (!(line.contains("JOB_FINISHED") && line.contains("org.apache.hadoop.mapreduce.jobhistory.JobFinished"))) continue;
				return line;
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}	
		return null;
	}
	
	public static void listFileNames(String directoryPath, Configuration conf, ArrayList<String> fileNameList) {
		try {
			FileSystem fs = FileSystem.get(new URI(directoryPath), conf);
			//fs.setPermission(new Path(directoryPath), new FsPermission("-rwxrwxrwx"));
			FileStatus[] status = fs.listStatus(new Path(directoryPath));
			for (FileStatus file : status) {
				if (file.isDirectory()) {
					listFileNames(file.getPath().toString(), conf, fileNameList);
				}
				if (!file.getPath().getName().endsWith(".jhist")) {
					continue;
				}
				//String filename = file.getPath().getName();
				String filename = file.getPath().toString();
				fileNameList.add(filename);
			}	
		} catch (IOException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}
}
