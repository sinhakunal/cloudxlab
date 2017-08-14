package com.cloudxlab.cardcount;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.cloudxlab.cardcount.datamodel.Card;

public class StubTest {

	/*
	 * Declare harnesses that let you test a mapper, a reducer, and a mapper and
	 * a reducer working together.
	 */
	MapDriver<Object, Text, IntWritable, LongWritable> mapDriver;
	ReduceDriver<IntWritable, LongWritable, Text, LongWritable> reduceDriver;
	MapReduceDriver<Object, Text, IntWritable, LongWritable, Text, LongWritable> mapReduceDriver;

	/*
	 * Set up the test. This method will be called before every test.
	 */
	@Before
	public void setUp() {

		/*
		 * Set up the mapper test harness.
		 */
		StubMapper mapper = new StubMapper();
		mapDriver = new MapDriver<Object, Text, IntWritable, LongWritable>();
		mapDriver.setMapper(mapper);

		/*
		 * Set up the reducer test harness.
		 */
		StubReducer reducer = new StubReducer();
		reduceDriver = new ReduceDriver<IntWritable, LongWritable, Text, LongWritable>();
		reduceDriver.setReducer(reducer);

		/*
		 * Set up the mapper/reducer test harness.
		 */
		mapReduceDriver = new MapReduceDriver<Object, Text, IntWritable, LongWritable, Text, LongWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	/*
	 * Test the mapper.
	 */
	@Test
	public void testMapper() {
		mapDriver.setInput("insignificant", new Text(""+Card.CLUBS+",2"));
		try {
			List<Pair<IntWritable, LongWritable>> out = mapDriver.run();
			assert(out.size()==1);
			int suite = out.get(0).getFirst().get();
			long value = out.get(0).getSecond().get();
			assertEquals(Card.CLUBS, suite);
			assertEquals(2, value);
		} catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}

	/*
	 * Test the reducer.
	 */
	@Test
	public void testReducer() {

		/*
		 * For this test, the reducer's input will be "cat 1 1". The expected
		 * output is "cat 2". TODO: implement
		 */
		//fail("Please implement test.");
		List<LongWritable> l = new ArrayList<LongWritable>();
		l.add(new LongWritable(2));
		l.add(new LongWritable(2));
		l.add(new LongWritable(3));
		l.add(new LongWritable(4));
		l.add(new LongWritable(15));

		reduceDriver.setInput(new IntWritable(Card.CLUBS), l);
		try {
			List<Pair<Text, LongWritable>> out = reduceDriver.run();
			Text okey = out.get(0).getFirst();
			LongWritable ov = out.get(0).getSecond();
			assertEquals(Card.getSuitAsString(Card.CLUBS), okey.toString());
			assertEquals(26, ov.get());
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/*
	 * Test the mapper and reducer working together.
	 */
	@Test
	public void testMapReduce() throws IOException {

		StringBuffer deck1 = new StringBuffer();
		deck1.append(new Card(Card.HEARTS, 10).getCSVOutput());

		StringBuffer deck2 = new StringBuffer();
		deck2.append(new Card(Card.JOKER, 0).getCSVOutput());

		StringBuffer deck3 = new StringBuffer();
		deck3.append(new Card(Card.DIAMONDS, 4).getCSVOutput());

		StringBuffer deck4 = new StringBuffer();
		deck4.append(new Card(Card.DIAMONDS, 4).getCSVOutput());

		mapReduceDriver.addInput(new Pair<Object, Text>("deck1", new Text(deck1.toString())));
		mapReduceDriver.addInput(new Pair<Object, Text>("deck2", new Text(deck2.toString())));
		mapReduceDriver.addInput(new Pair<Object, Text>("deck3", new Text(deck3.toString())));
		mapReduceDriver.addInput(new Pair<Object, Text>("deck4", new Text(deck4.toString())));
		List<Pair<Text, LongWritable>> output = mapReduceDriver.run();

		assertEquals(2, output.size());

		for (Pair<Text, LongWritable> p : output) {
			System.out.println(p.getFirst() + " - " + p.getSecond());
		}
	}
}
