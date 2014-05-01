
package cs455.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * More useful for inverse frequency.
 * @author Tyler
 *
 */
public class NGramMapper extends Mapper<Object, Text, Text, IntWritable> {

	private static final Pattern split = Pattern.compile("[^\\w'\\-\\$]+");

	private static IntWritable year;

	private static Text t = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

		String filename = ((FileSplit) context.getInputSplit()).getPath().getName();

		String syear = filename.split("Year")[1].replace(".txt", "");

		int iyear;
		if (syear.endsWith("BC")){
			iyear = -Integer.parseInt(syear.replace("BC", ""));//Negative year.
		} else {
			iyear = Integer.parseInt(syear);
		}

		year = new IntWritable((int) (Math.ceil(iyear/10)*10));

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
			t.set(word);
			context.write(t, year);
		}

		//N-Gram(2)
		for (int i = 0; i < words.size() - 1; ++i){
			t.set(words.get(i) + " " + words.get(i+1));
			context.write(t, year);
		}

		//N-Gram(3)
		for (int i = 0; i < words.size() - 2; ++i){
			t.set(words.get(i) + " " + words.get(i+1) + " " + words.get(i+2));
			context.write(t, year);
		}

	}

}
