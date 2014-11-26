package com.ruc.xx427.job.invertedIndex;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.ruc.xx427.job.entity.JobHistoryRecord;

/*
 * Class : InvertedIndex
 * Author: xiaohua
 * Time  : 2014/11/7 13:52
 * Use   : scan jobMeta.file and create inverted index, as jobname [1,2,4] which [1,2,4] are the offset
 * 		   eg : wordcount [1,40,190]
 */
public class InvertedIndex {
	
	public static Map<String, ArrayList<Long> > createIndex() {
		Map<String, ArrayList<Long>> index = new HashMap<String, ArrayList<Long>>();
		String metaFileName = JobHistoryRecord.getJobMetaFile();
		File file = new File(metaFileName);
		try {
			RandomAccessFile raf = new RandomAccessFile(file, "r");
			raf.seek(0);
			while (raf.getFilePointer() != raf.length()) {
				long currentOffset = raf.getFilePointer();
				int nameSize = raf.readInt();
				byte []tmp = new byte[nameSize];
				raf.read(tmp);
				String jobName = new String(tmp);
				raf.readLong();//long jobSize
				int idSize = raf.readInt();
				tmp = new byte[idSize];
				raf.read(tmp);
				//String jobID = new String(tmp);
				raf.readLong();//long offset 
				if (index.containsKey(jobName)) {
					index.get(jobName).add(currentOffset);
				} else {
					ArrayList<Long> list = new ArrayList<Long>();
					list.add(currentOffset);
					index.put(jobName, list);
				}
			}
			
			raf.close();
			return index;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
}
