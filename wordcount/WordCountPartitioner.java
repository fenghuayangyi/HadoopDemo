package com.bona.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<Text, IntWritable>{

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		
		// 1 获取单词key  
		String firWord = key.toString().substring(0, 1);
		char[] charArray = firWord.toCharArray();
		int result = charArray[0];
		// int result  = key.toString().charAt(0);

		// 2 根据奇数偶数分区
		if (result % 2 == 0) {
			return 0;
		}else {
			return 1;
		}
	}
}
