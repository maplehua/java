package com.ruc.xx427.job.entity;
/*
 * ClassName : FileSystemCounters
 * Author 	 : xiaohua
 * Time      : 2014/11/5 18:56
 * Use       : store the contents of the Map-Reduce Framework
 */
public class MapReduceFramework {

	private  final int NUM = 3;//means map counter, reduce counter, total counter
	public static final int Size = 8*MapReduceFramework.COUNT;//offset size for one object
	public static final int COUNT = 20;
	public  long combine_input_records[] = new long[NUM];
	public  long combine_output_records[] = new long[NUM];
	public  long cpu_time_spent[] = new long[NUM];
	public  long failed_shuffle[] = new long[NUM];
	public  long gc_time[] = new long[NUM];
	public  long input_split[] = new long[NUM];
	public  long map_input_records[] = new long[NUM];
	public  long map_output_bytes[] = new long[NUM];
	public  long map_output_mater[] = new long[NUM];
	public  long map_output_records[] = new long[NUM];
	public  long merge_map_outputs[] = new long[NUM];
	public  long physical_mem[] = new long[NUM];
	public  long reduce_input_groups[] = new long[NUM];
	public  long reduce_input_records[] = new long[NUM];
	public  long reduce_output_records[] = new long[NUM];
	public  long reduce_shuffle[] = new long[NUM];
	public  long shuffle_maps[] = new long[NUM];
	public  long spill_records[] = new long[NUM];
	public  long total_com_heap[] = new long[NUM];
	public  long virtual_mem[] = new long[NUM];
	
	public void printMapReduceFramework() {
		System.out.print("Combine input records"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(combine_input_records[i]+"\t");
		}
		System.out.println();
		
		System.out.print("Combine output records"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(combine_output_records[i]+"\t");
		}
		System.out.println();
		
		System.out.print("CPU time spent (ms)"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(cpu_time_spent[i]+"\t");
		}
		System.out.println();
		
		System.out.print("Failed Shuffles"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(failed_shuffle[i]+"\t");
		}
		System.out.println();
		
		System.out.print("GC time elapsed (ms)"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(gc_time[i]+"\t");
		}
		System.out.println();
		
		System.out.print("Input split bytes"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(input_split[i]+"\t");
		}
		System.out.println();
		
		System.out.print("Map input records"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(map_input_records[i]+"\t");
		}
		System.out.println();
		
		System.out.print("Map output bytes"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(map_output_bytes[i]+"\t");
		}
		System.out.println();
		
		System.out.print("Map output materialized bytes"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(map_output_mater[i]+"\t");
		}
		System.out.println();
		
		System.out.print("Map output records"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(map_output_records[i]+"\t");
		}
		System.out.println();
		
		System.out.print("Merged Map outputs"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(merge_map_outputs[i]+"\t");
		}
		System.out.println();
		
		System.out.print("Physical memory (bytes) snapshot"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(physical_mem[i]+"\t");
		}
		System.out.println();
		
		System.out.print("Reduce input groups"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(reduce_input_groups[i]+"\t");
		}
		System.out.println();
		
		System.out.print("Reduce input records"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(reduce_input_records[i]+"\t");
		}
		System.out.println();
		
		System.out.print("Reduce output records"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(reduce_output_records[i]+"\t");
		}
		System.out.println();
		
		System.out.print("Reduce shuffle bytes"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(reduce_shuffle[i]+"\t");
		}
		System.out.println();
		
		System.out.print("Shuffled Maps"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(shuffle_maps[i]+"\t");
		}
		System.out.println();
		
		System.out.print("Spilled Records"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(spill_records[i]+"\t");
		}
		System.out.println();
		
		System.out.print("Total committed heap usage (bytes)"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(total_com_heap[i]+"\t");
		}
		System.out.println();
		
		System.out.print("Virtual memory (bytes) snapshot"+"\t");
		for (int i = 0; i< NUM; i++) {
			System.out.print(virtual_mem[i]+"\t");
		}
		System.out.println();
	}
}
