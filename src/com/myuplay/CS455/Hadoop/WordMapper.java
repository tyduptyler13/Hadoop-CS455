package com.myuplay.CS455.Hadoop;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class WordMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);

	/**
	 * Matches any non alpha numeric character with the exception of - and '
	 */
	private final static Pattern pattern = Pattern.compile("[^a-zA-Z\\d\\s-']");

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

		String input = value.toString().toLowerCase();
		String[] words = pattern.matcher(input).replaceAll("").split("\\s+");//Precompiled regex.
		for (String word : words){
			context.write(new Text(word), one);
		}

	}

}
