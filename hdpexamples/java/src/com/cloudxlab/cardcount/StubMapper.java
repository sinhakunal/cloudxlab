package com.cloudxlab.cardcount;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;

import com.cloudxlab.cardcount.datamodel.Card;

public class StubMapper extends Mapper<Object, Text, IntWritable, LongWritable> {

	private static Log LOGGER = LogFactory.getLog(StubMapper.class);
	
  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
  	LOGGER.info("Entered map method"); 
  	LOGGER.info("key:" + key.toString());
  	LOGGER.info("value:" + value.toString());
	  String[] card = value.toString().split(",");
  	LOGGER.info("card"+card.toString());
	  Card c = new Card(Integer.parseInt(card[0]), Integer.parseInt(card[1]));
	  if(c.isNumberCard())
	  	context.write(new IntWritable(c.getSuit()), new LongWritable(c.getValue()));
  }
}
