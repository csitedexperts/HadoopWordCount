package com.csitedexperts.wordCount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Hello world!
 *
 */
public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	public void reduce (Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException
	{

		int sum = 0;
		Iterator <IntWritable> it = values.iterator();
		while(it.hasNext()) {
			sum +=it.next().get();

		}

		context.write(key,  new IntWritable(sum));

	}


}


