package com.ruc.xx427.job.entity;
/*
 * ClassName : FileSystemCounters
 * Author 	 : xiaohua 
 * Time      : 2014/11/5 18:38
 * Use       : store the contents of the File System Counter
 */
public class FileSystemCounters {

	private  final int NUM = 3;//means map counter, reduce counter, total counter
	public static final int Size = 8*FileSystemCounters.COUNT;//offset size for one object
	public static final int COUNT = 10;
	public  long file_number_read[] = new long[NUM];
	public  long file_number_write[] = new long[NUM];
	public  long file_number_large_read_op[] = new long[NUM];
	public  long file_number_read_op[] = new long[NUM];
	public  long file_number_write_op[] = new long[NUM];
	public  long hdfs_number_read[] = new long[NUM];
	public  long hdfs_number_write[] = new long[NUM];
	public  long hdfs_number_large_read_op[] = new long[NUM];
	public  long hdfs_number_read_op[] = new long[NUM];
	public  long hdfs_number_write_op[] = new long[NUM];
	
	public void printFileSystemCounters() {
		System.out.print("FILE: Number of bytes read"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(file_number_read[i]+"\t");
		}
		System.out.println(); 
		
		System.out.print("FILE: Number of bytes written"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(file_number_write[i]+"\t");
		}
		System.out.println();
		
		System.out.print("FILE: Number of large read operations"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(file_number_large_read_op[i]+"\t");
		}
		System.out.println();
		
		System.out.print("FILE: Number of read operations"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(file_number_read_op[i]+"\t");
		}
		System.out.println();
		
		System.out.print("FILE: Number of write operations"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(file_number_write_op[i]+"\t");
		}
		System.out.println();
		
		System.out.print("HDFS: Number of bytes read"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(hdfs_number_read[i]+"\t");
		}
		System.out.println();
		
		System.out.print("HDFS: Number of bytes written"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(hdfs_number_write[i]+"\t");
		}
		System.out.println();
		
		System.out.print("HDFS: Number of large read operations"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(hdfs_number_large_read_op[i]+"\t");
		}
		System.out.println();
		
		System.out.print("HDFS: Number of read operations"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(hdfs_number_read_op[i]+"\t");
		}
		System.out.println();
		
		System.out.print("HDFS: Number of write operations"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(hdfs_number_write_op[i]+"\t");
		}
		System.out.println();
	}
}
