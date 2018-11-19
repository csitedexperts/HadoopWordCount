package com.csitedexperts.wordCount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class WordCountApp 
{
	public static void main( String[] args )
	{
		System.out.println( "Hello World!" );

		try {
			Job job = Job.getInstance();
			job.setJobName("Word Count Job");
			job.setJarByClass(WordCountApp.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);;

			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path (args[0]));
			FileOutputFormat.setOutputPath(job,  new Path(args[1]));

			job.waitForCompletion(true);

		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}

