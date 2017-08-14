package com.cloudxlab.cardcount;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.cloudxlab.cardcount.datamodel.Card;

public class StubMapper extends Mapper<Object, Text, IntWritable, LongWritable> {

  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
	  String[] card = value.toString().split(",");
	  Card c = new Card(Integer.parseInt(card[0]), Integer.parseInt(card[1]));
	  if(c.isNumberCard())
	  	context.write(new IntWritable(c.getSuit()), new LongWritable(c.getValue()));
  }
}
