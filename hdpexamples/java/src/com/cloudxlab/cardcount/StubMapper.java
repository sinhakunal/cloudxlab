package com.cloudxlab.cardcount;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.cloudxlab.cardcount.datamodel.Card;

public class StubMapper extends Mapper<Object, List<Card>, IntWritable, LongWritable> {

  @Override
  public void map(Object key, List<Card> cards, Context context)
      throws IOException, InterruptedException {

	  //iterate over the cards
	  for(Card card:cards)
	  {
	  	if(card.isNumberCard()) {
	  		context.write(new IntWritable(card.getSuit()), new LongWritable(card.getValue()));
	  	}
	  }
  }
}
