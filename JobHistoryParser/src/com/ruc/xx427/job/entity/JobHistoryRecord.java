package com.ruc.xx427.job.entity;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.ruc.xx427.property.Property;

/*
 * ClassName : JobHistoryRecord
 * Author 	 : xiaohua
 * Time      : 2014/11/5 17:12
 * Use       : every instance of this class represents a special Job History log
 *             It stores all the data of a job
 */
public class JobHistoryRecord {

	public static final int MAP = 0;
	public static final int REDUCE = 1;
	public static final int TOTAL = 2;
	private static final int NUM = 3;
	//data set to collect
	private FileSystemCounters fileSystemCounters;
	private MapReduceFramework mapReduceFramework;
	
	//some variable to write file. meta file and log file
	private String JobID;
	private String JobName;
	private long JobSize;
	//JobMeta file name and JobCounter file name
	//content: jobname jobsize jobid offset
	private static String jobMetaFile = Property.JobMetaFile;
	
	//jobid file system counters mapredue framwork
	private static String jobCounterFile = Property.JobCounterFile;
	private static RandomAccessFile raf_counter;
	private static RandomAccessFile raf_meta;
	
	//estimated map and reduce time (added by Juwei)
	//XXX if we want to remove these fields out of JobHistoryRecord
	private float mapFunTime; //map function, sort, and compress tim, MB/sec
	private float reduceFunTime; //reduce and decompress time, MB/sec
	
	public static void init() {
		try {
			raf_counter = new RandomAccessFile(new File(jobCounterFile), "rw");
			raf_meta = new RandomAccessFile(new File(jobMetaFile), "rw");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public static void close() {
		try {
			raf_counter.close();
			raf_meta.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static String getJobMetaFile() {
		return jobMetaFile;
	}
	
	public FileSystemCounters getFileSystemCounters() {
		return fileSystemCounters;
	}

	public MapReduceFramework getMapReduceFramework() {
		return mapReduceFramework;
	}
	public String getJobID() {
		return JobID;
	}
	public void setJobID(String jobID) {
		JobID = jobID;
	}
	public String getJobName() {
		return JobName;
	}
	public void setJobName(String jobName) {
		JobName = jobName;
	}
	public long getJobSize() {
		return JobSize;
	}
	public void setJobSize(long jobSize) {
		JobSize = jobSize;
	}
	
	public float getMapFunTime() {
		return mapFunTime;
	}

	public void setMapFunTime(float mapFunTime) {
		this.mapFunTime = mapFunTime;
	}

	public float getReduceFunTime() {
		return reduceFunTime;
	}

	public void setReduceFunTime(float reduceFunTime) {
		this.reduceFunTime = reduceFunTime;
	}
	
	
	public JobHistoryRecord() {
		fileSystemCounters = new FileSystemCounters();
		mapReduceFramework = new MapReduceFramework();
		JobID = "";
		JobName = "";
		JobSize = 0;
	}
	private static void writeArray(long array[]) throws IOException {
		JobHistoryRecord.raf_counter.seek(JobHistoryRecord.raf_counter.length());
		for (int i = 0; i< NUM; i++) {
			JobHistoryRecord.raf_counter.writeLong(array[i]);
		}
	}
	private static void writeFileSystemCounters(JobHistoryRecord jobHistRecord) {
		try {
			JobHistoryRecord.raf_counter.seek(JobHistoryRecord.raf_counter.length());
			writeArray(jobHistRecord.fileSystemCounters.file_number_read);
			writeArray(jobHistRecord.fileSystemCounters.file_number_write);
			writeArray(jobHistRecord.fileSystemCounters.file_number_large_read_op);
			writeArray(jobHistRecord.fileSystemCounters.file_number_read_op);
			writeArray(jobHistRecord.fileSystemCounters.file_number_write_op);
			writeArray(jobHistRecord.fileSystemCounters.hdfs_number_read);
			writeArray(jobHistRecord.fileSystemCounters.hdfs_number_write);
			writeArray(jobHistRecord.fileSystemCounters.hdfs_number_large_read_op);
			writeArray(jobHistRecord.fileSystemCounters.hdfs_number_read_op);
			writeArray(jobHistRecord.fileSystemCounters.hdfs_number_write_op);	
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	private static void writeMapReduceFramework(JobHistoryRecord jobHistRecord) {
		try {
			JobHistoryRecord.raf_counter.seek(JobHistoryRecord.raf_counter.length());
			writeArray(jobHistRecord.mapReduceFramework.combine_input_records);
			writeArray(jobHistRecord.mapReduceFramework.combine_output_records);
			writeArray(jobHistRecord.mapReduceFramework.cpu_time_spent);
			writeArray(jobHistRecord.mapReduceFramework.failed_shuffle);
			writeArray(jobHistRecord.mapReduceFramework.gc_time);
			writeArray(jobHistRecord.mapReduceFramework.input_split);
			writeArray(jobHistRecord.mapReduceFramework.map_input_records);
			writeArray(jobHistRecord.mapReduceFramework.map_output_bytes);
			writeArray(jobHistRecord.mapReduceFramework.map_output_mater);
			writeArray(jobHistRecord.mapReduceFramework.map_output_records);
			writeArray(jobHistRecord.mapReduceFramework.merge_map_outputs);
			writeArray(jobHistRecord.mapReduceFramework.physical_mem);
			writeArray(jobHistRecord.mapReduceFramework.reduce_input_groups);
			writeArray(jobHistRecord.mapReduceFramework.reduce_input_records);
			writeArray(jobHistRecord.mapReduceFramework.reduce_output_records);
			writeArray(jobHistRecord.mapReduceFramework.reduce_shuffle);
			writeArray(jobHistRecord.mapReduceFramework.shuffle_maps);
			writeArray(jobHistRecord.mapReduceFramework.spill_records);
			writeArray(jobHistRecord.mapReduceFramework.total_com_heap);
			writeArray(jobHistRecord.mapReduceFramework.virtual_mem);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/*
	 * @param : JobHistoryRecord, the object to write to the file
	 * @return : void
	 * @JobMeta file 
	 * content: jobname jobsize jobid offset
	 * @JobCounters
	 * content: file system counters , mapredue framwork
	 */
	public static void write(JobHistoryRecord jobHistRecord) throws IOException {
		//==================write job counters file===============
		//the pointer offset in jobMeta
		JobHistoryRecord.raf_counter.seek(JobHistoryRecord.raf_counter.length());
		long pointer = JobHistoryRecord.raf_counter.length();
		//for now , not write the jobID , for easy to read
		//JobHistoryRecord.raf_counter.writeInt(jobHistRecord.JobID.getBytes().length);
		//JobHistoryRecord.raf_counter.writeChars(jobHistRecord.JobID);
		JobHistoryRecord.writeFileSystemCounters(jobHistRecord);
		JobHistoryRecord.writeMapReduceFramework(jobHistRecord);
		//=================write job meta file=====================
		JobHistoryRecord.raf_meta.seek(JobHistoryRecord.raf_meta.length());
		//the offset and value of the jobName
		JobHistoryRecord.raf_meta.writeInt(jobHistRecord.JobName.getBytes().length);
		JobHistoryRecord.raf_meta.writeBytes(jobHistRecord.JobName);
		//the value of the jobSize
		JobHistoryRecord.raf_meta.writeLong(jobHistRecord.JobSize);
		//the offset and value of jobID
		JobHistoryRecord.raf_meta.writeInt(jobHistRecord.JobID.getBytes().length);
		JobHistoryRecord.raf_meta.writeBytes(jobHistRecord.JobID);
		//the value of the pointer (long)
		JobHistoryRecord.raf_meta.writeLong(pointer);
		
	}
	
	private static long[] readArray() {
		long tmp[] = new long[JobHistoryRecord.NUM];
		try {
			for (int i = 0 ; i< JobHistoryRecord.NUM; i++) {
				tmp[i] = JobHistoryRecord.raf_counter.readLong();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return tmp;
	}
	private static void readFileSystemCounters(JobHistoryRecord jobHistRecord, long offset) throws IOException {
		JobHistoryRecord.raf_counter.seek(offset);
		jobHistRecord.fileSystemCounters.file_number_read = readArray();
		jobHistRecord.fileSystemCounters.file_number_write = readArray();
		jobHistRecord.fileSystemCounters.file_number_large_read_op = readArray();
		jobHistRecord.fileSystemCounters.file_number_read_op = readArray();
		jobHistRecord.fileSystemCounters.file_number_write_op = readArray();
		jobHistRecord.fileSystemCounters.hdfs_number_read = readArray();
		jobHistRecord.fileSystemCounters.hdfs_number_write = readArray();
		jobHistRecord.fileSystemCounters.hdfs_number_large_read_op = readArray();
		jobHistRecord.fileSystemCounters.hdfs_number_read_op = readArray();
		jobHistRecord.fileSystemCounters.hdfs_number_write_op = readArray();
	}
	private static void readMapReduceFramework(JobHistoryRecord jobHistRecord, long offset) {
		try {
			JobHistoryRecord.raf_counter.seek(offset);
			jobHistRecord.mapReduceFramework.combine_input_records = readArray();
			jobHistRecord.mapReduceFramework.combine_output_records = readArray();
			jobHistRecord.mapReduceFramework.cpu_time_spent = readArray();
			jobHistRecord.mapReduceFramework.failed_shuffle = readArray();;
			jobHistRecord.mapReduceFramework.gc_time = readArray();
			jobHistRecord.mapReduceFramework.input_split = readArray();
			jobHistRecord.mapReduceFramework.map_input_records = readArray();
			jobHistRecord.mapReduceFramework.map_output_bytes = readArray();
			jobHistRecord.mapReduceFramework.map_output_mater = readArray();
			jobHistRecord.mapReduceFramework.map_output_records = readArray();
			jobHistRecord.mapReduceFramework.merge_map_outputs = readArray();
			jobHistRecord.mapReduceFramework.physical_mem = readArray();
			jobHistRecord.mapReduceFramework.reduce_input_groups = readArray();
			jobHistRecord.mapReduceFramework.reduce_input_records = readArray();
			jobHistRecord.mapReduceFramework.reduce_output_records = readArray();
			jobHistRecord.mapReduceFramework.reduce_shuffle = readArray();
			jobHistRecord.mapReduceFramework.shuffle_maps = readArray();
			jobHistRecord.mapReduceFramework.spill_records = readArray();
			jobHistRecord.mapReduceFramework.total_com_heap = readArray();
			jobHistRecord.mapReduceFramework.virtual_mem = readArray();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	/*
	 * @param : offset , the offset in the job meta file to read
	 * @return : boolean
	 */
	public static JobHistoryRecord read(long offset) throws IOException {
		JobHistoryRecord tmpJobHistRecord = new JobHistoryRecord();
		JobHistoryRecord.raf_meta.seek(offset);
		int nameSize = JobHistoryRecord.raf_meta.readInt();
		byte tmp[] = new byte[nameSize];
		JobHistoryRecord.raf_meta.read(tmp);
		tmpJobHistRecord.setJobName(new String(tmp));
		tmpJobHistRecord.setJobSize(JobHistoryRecord.raf_meta.readLong());
		int idSize = JobHistoryRecord.raf_meta.readInt();
		byte tmp1[] = new byte[idSize];
		JobHistoryRecord.raf_meta.read(tmp1);
		tmpJobHistRecord.setJobID(new String(tmp1));
		long pointer = JobHistoryRecord.raf_meta.readLong();
		readFileSystemCounters(tmpJobHistRecord, pointer);
		readMapReduceFramework(tmpJobHistRecord, pointer+FileSystemCounters.Size);//long 8 bytes
		return tmpJobHistRecord;
	}
	
	/*
	 * @param keyName : the key name in history log, like "FILE: Number of bytes read"
	 * @param counterKind : the kind of counter, such as total counters = 2, map counters = 0, reduce counters = 1
	 * @param value : the value to set , which is read from history log
	 * @return : boolean
	 * use     : to set the special value of the Class FileSystemCounters
	 */
	public boolean setFileSystemCounters(String keyName, int counterKind, long value) {
		if (keyName.startsWith("FILE: Number of bytes read")) {
			this.fileSystemCounters.file_number_read[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("FILE: Number of bytes written")) {
			this.fileSystemCounters.file_number_write[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("FILE: Number of large read operations")) {
			this.fileSystemCounters.file_number_large_read_op[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("FILE: Number of read operations")) {
			this.fileSystemCounters.file_number_read_op[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("FILE: Number of write operations")) {
			this.fileSystemCounters.file_number_write_op[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("HDFS: Number of bytes read")) {
			this.fileSystemCounters.hdfs_number_read[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("HDFS: Number of bytes written")) {
			this.fileSystemCounters.hdfs_number_write[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("HDFS: Number of large read operations")) {
			this.fileSystemCounters.hdfs_number_large_read_op[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("HDFS: Number of read operations")) {
			this.fileSystemCounters.hdfs_number_read_op[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("HDFS: Number of write operations")) {
			this.fileSystemCounters.hdfs_number_write_op[counterKind] = value;
			return true;
		}
	
		return false;
	}
	/*
	 * @param keyName : the key name in history log, like "Combine input records"
	 * @param counterKind : the kind of counter, such as total counters = 2, map counters = 0, reduce counters = 1
	 * @param value : the value to set , which is read from history log
	 * @return : boolean
	 * use     : to set the special value of the Class MapReduceFramework
	 */
	public boolean setMapReduceFramework(String keyName, int counterKind, long value) {
		if (keyName.startsWith("Combine input records")) {
			this.mapReduceFramework.combine_input_records[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("Combine output records")) {
			this.mapReduceFramework.combine_output_records[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("CPU time spent (ms)")) {
			this.mapReduceFramework.cpu_time_spent[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("Failed Shuffles")) {
			this.mapReduceFramework.failed_shuffle[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("GC time elapsed (ms)")) {
			this.mapReduceFramework.gc_time[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("Input split bytes")) {
			this.mapReduceFramework.input_split[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("Map input records")) {
			this.mapReduceFramework.map_input_records[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("Map output bytes")) {
			this.mapReduceFramework.map_output_bytes[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("Map output materialized bytes")) {
			this.mapReduceFramework.map_output_mater[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("Map output records")) {
			this.mapReduceFramework.map_output_records[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("Merged Map outputs")) {
			this.mapReduceFramework.merge_map_outputs[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("Physical memory (bytes) snapshot")) {
			this.mapReduceFramework.physical_mem[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("Reduce input groups")) {
			this.mapReduceFramework.reduce_input_groups[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("Reduce input records")) {
			this.mapReduceFramework.reduce_input_records[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("Reduce output records")) {
			this.mapReduceFramework.reduce_output_records[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("Reduce shuffle bytes")) {
			this.mapReduceFramework.reduce_shuffle[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("Shuffled Maps")) {
			this.mapReduceFramework.shuffle_maps[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("Spilled Records")) {
			this.mapReduceFramework.spill_records[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("Total committed heap usage (bytes)")) {
			this.mapReduceFramework.total_com_heap[counterKind] = value;
			return true;
		}
		if (keyName.startsWith("Virtual memory (bytes) snapshot")) {
			this.mapReduceFramework.virtual_mem[counterKind] = value;
			return true;
		}
		return false;
	}
}


