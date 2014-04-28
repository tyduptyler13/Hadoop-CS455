package com.myuplay.CS455.Hadoop;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class WordMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

		String input = value.toString();
		String[] words = input.toLowerCase().replaceAll("[^a-z0-9-]", "").split("\\s+");
		for (String word : words){
			context.write(new Text(word), one);
		}

	}

}
