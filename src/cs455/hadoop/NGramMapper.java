package cs455.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class NGramMapper extends Mapper<Object, Text, Text, IntWritable> {

	private static final IntWritable one = new IntWritable(1);
	private static final Pattern split = Pattern.compile("[^\\w'\\-\\$]+");

	private static Text t = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

		String file = ((FileSplit) context.getInputSplit()).getPath().getName();

		String input = value.toString().toUpperCase(Locale.ENGLISH);
		String[] unfiltered = split.split(input);

		List<String> words = new ArrayList<String>();
		//Filter once
		for (String word : unfiltered){
			if (word.length() > 0){
				words.add(word.trim()); //Ensured to be clean now.
			}
		}

		//The following could be improved for better memory access.

		//N-Gram(1)
		for (String word : words){
			t.set(file + " " + word);
			context.write(t, one);
		}

		//N-Gram(2)
		for (int i = 0; i < words.size() - 1; ++i){
			t.set(file + " " + words.get(i) + " " + words.get(i+1));
			context.write(t, one);
		}

		//N-Gram(3)
		for (int i = 0; i < words.size() - 2; ++i){
			t.set(file + " " + words.get(i) + " " + words.get(i+1) + " " + words.get(i+2));
			context.write(t, one);
		}

	}

}
