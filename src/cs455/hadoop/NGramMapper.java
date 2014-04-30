package cs455.hadoop;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class NGramMapper extends Mapper<Object, Text, Text, IntWritable> {

	private static final IntWritable one = new IntWritable(1);
	private static final Pattern replace = Pattern.compile("[^a-zA-Z\\d\\s':\\$\\-]");
	private static final Pattern split = Pattern.compile("[\\s\t\\n]+");

	private static final String space = " ";
	
	private static Text t = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

		String input = value.toString().toLowerCase();
		input = replace.matcher(input).replaceAll(space);//Precompiled regex.
		String[] words = split.split(input);
		
		//N-Gram(1)
		for (String word : words){
			t.set(word);
			context.write(t, one);
		}
		
		//N-Gram(2)
		for (int i = 0; i < words.length - 1; ++i){
			t.set(words[i] + " " + words[i+1]);
			context.write(t, one);
		}
		
		//N-Gram(3)
		for (int i = 0; i < words.length - 2; ++i){
			t.set(words[i] + " " + words[i+1] + " " + words[i+2]);
			context.write(t, one);
		}

	}

}
