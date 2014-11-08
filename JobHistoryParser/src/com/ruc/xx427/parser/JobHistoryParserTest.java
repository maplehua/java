package com.ruc.xx427.parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import com.ruc.xx427.entity.JobHistoryRecord;

public class JobHistoryParserTest {

	public static void main(String[] args) {
		try {
			File f = new File("I:\\log.txt");
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
			String log = br.readLine();
			JobHistoryRecord jobHistRecord = JobHistoryParser.parseLog(log);
			jobHistRecord.getFileSystemCounters().printFileSystemCounters();
			jobHistRecord.getMapReduceFramework().printMapReduceFramework();
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
