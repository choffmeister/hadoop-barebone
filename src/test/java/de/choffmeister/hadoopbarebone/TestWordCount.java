package de.choffmeister.hadoopbarebone;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.*;
import org.junit.Test;
import org.junit.Before;

public class TestWordCount {

	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		WordCountMapper mapper = new WordCountMapper();
		WordCountReducer reducer = new WordCountReducer();

		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapper() throws Exception {
		mapDriver.withInput(new LongWritable(), new Text("fish foobar fish"));
		
		mapDriver.withOutput(new Text("fish"), new IntWritable(1));
		mapDriver.withOutput(new Text("foobar"), new IntWritable(1));
		mapDriver.withOutput(new Text("fish"), new IntWritable(1));
		
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws Exception {
		reduceDriver.withInput(new Text("three"), CreateIntList(1, 1, 1));
		reduceDriver.withInput(new Text("two"), CreateIntList(1, 1));
		reduceDriver.withInput(new Text("one"), CreateIntList(1));
		reduceDriver.withInput(new Text("five"), CreateIntList(3, 2));
		
		reduceDriver.withOutput(new Text("three"), new IntWritable(3));
		reduceDriver.withOutput(new Text("two"), new IntWritable(2));
		reduceDriver.withOutput(new Text("one"), new IntWritable(1));
		reduceDriver.withOutput(new Text("five"), new IntWritable(5));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapperReducer() throws Exception {
		mapReduceDriver.withInput(new LongWritable(), new Text("one fish two fish"));
		mapReduceDriver.withInput(new LongWritable(), new Text("red fish blue fish"));

		mapReduceDriver.withOutput(new Text("one"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("fish"), new IntWritable(4));
		mapReduceDriver.withOutput(new Text("two"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("red"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("blue"), new IntWritable(1));
	}

	/**
	 * Takes a variable count of Integer parameters and returns the corrosponding
	 * list of IntWritable objects.
	 * 
	 * @param integers The integers.
	 * @return The IntWritable list.
	 */
	private static List<IntWritable> CreateIntList(Integer... integers) {
		List<IntWritable> result = new ArrayList<IntWritable>();
	
		for (Integer i : integers) {
			result.add(new IntWritable(i));
		}
		
		return result;
	}
}
