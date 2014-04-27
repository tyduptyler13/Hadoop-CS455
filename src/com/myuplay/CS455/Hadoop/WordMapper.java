package com.myuplay.CS455.Hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class WordMapper extends Mapper<Object, Text, Text, IntWritable> {

	private Text word = new Text();
	private final static IntWritable one = new IntWritable(1);

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

		StringTokenizer wordList = new StringTokenizer(value.toString());
		while (wordList.hasMoreTokens()){
			word.set(wordList.nextToken());
			context.write(word, one);
		}

	}

}
