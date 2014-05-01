package cs455.hadoop;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NGramReducer extends Reducer<Text, IntWritable, Text, Text> {

	//Sorted on year.
	private static final TreeMap<IntWritable, IntWritable> wordsByDecade = new TreeMap<IntWritable, IntWritable>();

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

		for (IntWritable value : values){
			increment(value);
		}

		for (IntWritable decade : wordsByDecade.keySet()){
			//NGram|year|count
			context.write(key, new Text(decade.toString() + "\t" + wordsByDecade.get(decade).toString()));
		}

	}

	/**
	 * Increments the words use in a decade.
	 * @param year
	 */
	private void increment(IntWritable year){
		IntWritable value = wordsByDecade.get(year);
		value.set(value.get()+1);
	}

}
