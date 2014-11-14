package com.ruc.xx437.profile.data;
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
	
	

}
