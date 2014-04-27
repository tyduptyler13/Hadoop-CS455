package com.myuplay.CS455.Hadoop;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	private IntWritable totalWordCount = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

		int wordCount = 0;

		for (IntWritable cur : values){

			wordCount += cur.get();

		}

		totalWordCount.set(wordCount);

		context.write(key, totalWordCount);

	}

}
