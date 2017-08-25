package com.cloudxlab.cardcount;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.cloudxlab.cardcount.datamodel.Card;

public class StubReducer extends Reducer<IntWritable, LongWritable, Text, LongWritable> {

	private static Log LOGGER = LogFactory.getLog(StubMapper.class);
	
  @Override
  public void reduce(IntWritable key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
  	int keyProcess = key.get();
  	LOGGER.info("Processing for key:"+keyProcess);
	  long sum = 0;
	  if(keyProcess != Card.JOKER) {
		  for(LongWritable iw:values)
		  {
			  sum += iw.get();
		  }
	  }
	  LOGGER.info("Sum for suit "+keyProcess+ " is:"+sum);
	  context.write(new Text(Card.getSuitAsString(keyProcess)), new LongWritable(sum));
  }
}
