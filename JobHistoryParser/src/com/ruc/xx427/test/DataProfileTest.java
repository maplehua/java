package com.ruc.xx427.test;

import com.ruc.xx427.profile.data.DataProfile;

public class DataProfileTest {

	public static void main(String[] args) {
		DataProfile dataProfile = new DataProfile();
		dataProfile = dataProfile.setDataInfo("hdfs://10.77.50.165:9000/user/xx427/terasort/10000000-input");
		System.out.println(dataProfile.getBlockSize());
		System.out.println(dataProfile.getDataSize());
		System.out.println(dataProfile.getInputPath());
		System.out.println(dataProfile.getNumOfFiles());

	}

}
