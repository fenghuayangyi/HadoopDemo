package com.bona.mapreduce.flowsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPartitioner extends Partitioner<FlowBean, Text>{

	@Override
	public int getPartition(FlowBean value, Text key, int numPartitions) {
		
		
		// 1 需求：根据电话号码的前3位是几来分区
		
		// 拿到电话号码的前3位
		String phoneNum = key.toString().substring(0, 3);
		
		int partitions = 4;
		
		if ("135".equals(phoneNum)) {
			partitions = 0;
		}else if ("136".equals(phoneNum)) {
			partitions = 1;
		}else if ("137".equals(phoneNum)) {
			partitions = 2;
		}else if ("138".equals(phoneNum)) {
			partitions = 3;
		}
		
		return partitions;
	}

}
