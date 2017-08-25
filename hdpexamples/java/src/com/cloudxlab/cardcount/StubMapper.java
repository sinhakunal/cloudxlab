package com.cloudxlab.cardcount;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.cloudxlab.cardcount.datamodel.Card;

public class StubMapper extends Mapper<Object, Text, IntWritable, LongWritable> {

	private static Logger LOGGER = Logger.global;
	
  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
  	LOGGER.log(Level.INFO, "Entered map method");
  	LOGGER.log(Level.INFO, "key:" + key.toString());
  	LOGGER.log(Level.INFO, "value:" + value.toString());
	  String[] card = value.toString().split(",");
  	LOGGER.log(Level.INFO, "card"+card.toString());
	  Card c = new Card(Integer.parseInt(card[0]), Integer.parseInt(card[1]));
	  if(c.isNumberCard())
	  	context.write(new IntWritable(c.getSuit()), new LongWritable(c.getValue()));
  }
}
