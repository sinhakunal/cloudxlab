package com.cloudxlab.cardcount;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.cloudxlab.cardcount.datamodel.Card;

public class StubReducer extends Reducer<IntWritable, LongWritable, Text, LongWritable> {

  @Override
  public void reduce(IntWritable key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {

	  long sum = 0;
	  for(LongWritable iw:values)
	  {
		  sum += iw.get();
	  }
	  context.write(new Text(Card.getSuitAsString(key.get())), new LongWritable(sum));
  }
}
