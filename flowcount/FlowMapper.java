package com.bona.mapreduce.flowcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
	FlowBean bean = new FlowBean();
	Text k = new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 1 ��ȡһ������
		String line = value.toString();
		
		// 2 ��ȡ�ֶ�
		String[] fields = line.split("\t");
		
		// 3 ��װbean�����Լ���ȡ�绰��
		String phoneNum = fields[1];
		
		long upFlow = Long.parseLong(fields[fields.length - 3]);
		long downFlow = Long.parseLong(fields[fields.length - 2]);
	
		bean.set(upFlow, downFlow);
		k.set(phoneNum);
		
		// 4 д��ȥ
		context.write(k, bean);
	}
	
	
}
