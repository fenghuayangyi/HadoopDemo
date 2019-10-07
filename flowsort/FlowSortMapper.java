package com.bona.mapreduce.flowsort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// ��������ݣ�13480253104	180	180	360
public class FlowSortMapper extends Mapper<LongWritable, Text, FlowBean, Text>{
	FlowBean bean = new FlowBean();
	Text v = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 1 ��ȡһ��
		String line = value.toString();
		
		// 2 ��ȡ�ֶ�
		String[] fields = line.split("\t");
		
		// 3 ��װ���󼰻�ȡ�绰����
		long upFlow = Long.parseLong(fields[1]);
		long downFlow = Long.parseLong(fields[2]);
		
		bean.set(upFlow, downFlow);
		v.set(fields[0]);
		
		// 4 д��ȥ
		context.write(bean, v);
		
	}
}
