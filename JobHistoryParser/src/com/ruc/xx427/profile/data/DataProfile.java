package com.ruc.xx427.profile.data;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * Description: the profile entity for data from a given path
 *
 * @author Juwei
 * @date 2014-11-14
 *
 */
public class DataProfile {

	private long dataSize; // bytes
	private long blockSize;// bytes;
	private String inputPath;
	private int numOfFiles;
	private boolean first = true;
	public long getDataSize() {
		return dataSize;
	}
	public void setDataSize(long dataSize) {
		this.dataSize = dataSize;
	}
	public long getBlockSize() {
		return blockSize;
	}
	public void setBlockSize(long blockSize) {
		this.blockSize = blockSize;
	}
	public String getInputPath() {
		return inputPath;
	}
	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}
	public int getNumOfFiles() {
		return numOfFiles;
	}
	public void setNumOfFiles(int numOfFiles) {
		this.numOfFiles = numOfFiles;
	}
	
	//add by xiaohua at 2014/11/29
	public DataProfile setDataInfo(String dataPath) {
		//dataPath must be a full path like hdfs://10.77.50.165:9000/user/xx427/terasort/10000000-input
		//maybe modify it if necessary
		DataProfile dataProfile = new DataProfile();
		FileSystem fs;
		try {
			fs = null;
			fs = FileSystem.get(URI.create(dataPath), new Configuration());
			getDataInfo(fs, dataPath);
			dataProfile.setDataSize(this.dataSize);
			dataProfile.setBlockSize(this.blockSize);
			dataProfile.setNumOfFiles(this.numOfFiles);
			dataProfile.setInputPath(this.inputPath);
			
			//for next
			this.dataSize = 0;
			this.blockSize = 0;
			this.numOfFiles = 0;
			this.inputPath = "";
		} catch (IOException e) {
			e.printStackTrace();
		}
		return dataProfile;
	}
	
	private void getDataInfo(FileSystem fs, String dataPath) {
		Path workDir = fs.getWorkingDirectory();
		Path dst;
		if (null == dataPath.toString() || "".equals(dataPath.toString())) {
			dst = new Path(workDir + "/" + dataPath);
		} else {
			dst = new Path(dataPath);
		}
		
		inputPath = dst.toString();
		
		String relativePath = "";
		FileStatus[] fList = null;
		try {
			fList = fs.listStatus(dst);
			numOfFiles = fList.length;
			for (FileStatus f : fList) {
				if (null != f) {
					// System.out.println(f.getPermission());
					relativePath = new StringBuffer()
							.append(f.getPath().getParent()).append("/")
							.append(f.getPath().getName()).toString();
					if (f.isDirectory()) {
						getDataInfo(fs, relativePath);
					} else {
						dataSize += f.getLen();
						if (first) {
							blockSize = f.getBlockSize();
							first = false;
						}
						if (blockSize != f.getBlockSize()) {
							blockSize = 0;
						}
					}
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}

}
