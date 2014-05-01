package cs455.hadoop;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NGramReducer extends Reducer<Text, IntWritable, Text, Text> {

	//Sorted on year.
	private TreeMap<Integer, Integer> wordsByDecade = new TreeMap<Integer, Integer>();

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

		wordsByDecade.clear(); //Ensure our map is empty.

		//Read in the years that word shows up.
		//Increment a counter on that year.
		for (IntWritable value : values){
			increment(value);
		}

		for (Integer decade : wordsByDecade.keySet()){
			//NGram|year|count
			context.write(key, new Text(printDecade(decade) + "\t" + wordsByDecade.get(decade).toString()));
		}

	}

	/**
	 * Increments the words use in a decade.
	 * @param year
	 */
	private void increment(IntWritable year){

		Integer value = wordsByDecade.get(year.get());

		if (value == null){
			value = new Integer(0);
		}

		wordsByDecade.put(year.get(), new Integer(value.intValue() + 1));

	}

	private static String printDecade(Integer decade){
		if (decade < 0){
			return decade.toString() + "BC";
		} else {
			return decade.toString();
		}
	}

}
