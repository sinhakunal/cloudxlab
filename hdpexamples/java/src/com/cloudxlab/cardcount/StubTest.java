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
	MapDriver<Object, List<Card>, IntWritable, LongWritable> mapDriver;
	ReduceDriver<IntWritable, LongWritable, Text, LongWritable> reduceDriver;
	MapReduceDriver<Object, List<Card>, IntWritable, LongWritable, Text, LongWritable> mapReduceDriver;

	/*
	 * Set up the test. This method will be called before every test.
	 */
	@Before
	public void setUp() {

		/*
		 * Set up the mapper test harness.
		 */
		StubMapper mapper = new StubMapper();
		mapDriver = new MapDriver<Object, List<Card>, IntWritable, LongWritable>();
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
		mapReduceDriver = new MapReduceDriver<Object, List<Card>, IntWritable, LongWritable, Text, LongWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	/*
	 * Test the mapper.
	 */
	@Test
	public void testMapper() {

		/*
		 * For this test, the mapper's input will be "1 cat cat dog" TODO:
		 * implement
		 */
		List<Card> cards = new ArrayList<Card>();
		cards.add(new Card(1, Card.HEARTS));
		cards.add(new Card(10, Card.CLUBS));
		cards.add(new Card(13, Card.CLUBS));
		mapDriver.setInput("insignificant", cards);
		try {
			List<Pair<IntWritable, LongWritable>> out = mapDriver.run();
			assert(out.size()==1);
			int suite = out.get(0).getFirst().get();
			long value = out.get(0).getSecond().get();
			assertEquals(Card.CLUBS, suite);
			assertEquals(10, value);
		} catch(Exception e) {
			fail("Exception");
			e.printStackTrace();
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

		List<Card> deck1 = new ArrayList<Card>();
		deck1.add(new Card(1, Card.HEARTS));
		deck1.add(new Card(10, Card.CLUBS));
		deck1.add(new Card(13, Card.CLUBS));

		List<Card> deck2 = new ArrayList<Card>();
		deck1.add(new Card(0, Card.JOKER));
		deck1.add(new Card(8, Card.HEARTS));
		deck1.add(new Card(7, Card.SPADES));

		List<Card> deck3 = new ArrayList<Card>();
		deck3.add(new Card(4, Card.DIAMONDS));
		deck3.add(new Card(4, Card.DIAMONDS));
		deck3.add(new Card(4, Card.HEARTS));

		mapReduceDriver.addInput(new Pair<Object, List<Card>>("deck1", deck1));
		mapReduceDriver.addInput(new Pair<Object, List<Card>>("deck2", deck2));
		mapReduceDriver.addInput(new Pair<Object, List<Card>>("deck3", deck3));
		List<Pair<Text, LongWritable>> output = mapReduceDriver.run();

		assertEquals(4, output.size());

		for (Pair<Text, LongWritable> p : output) {
			System.out.println(p.getFirst() + " - " + p.getSecond());
		}
	}
}
