package com.cloudxlab.cardcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class StubPartitioner extends Partitioner<IntWritable, LongWritable>
{
	@Override
	public int getPartition(IntWritable key, LongWritable arg1, int numReducers) {
		return key.get() % 4;
	}
}
