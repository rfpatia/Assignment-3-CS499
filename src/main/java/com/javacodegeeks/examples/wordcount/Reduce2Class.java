package com.javacodegeeks.examples.wordcount;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce class which is executed after the map class and takes
 * key(word) and corresponding values, sums all the values and write the
 * word along with the corresponding total occurances in the output
 * 
 * @author Raman
 */
public class Reduce2Class extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

	/**
	 * Method which performs the reduce operation and sums 
	 * all the occurrences of the word before passing it to be stored in output
	 */
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Context context)
			throws IOException, InterruptedException {
	
		double sum = 0;
		int count = 0;
		Iterator<DoubleWritable> valuesIt = values.iterator();
		
		while(valuesIt.hasNext()){
			count++;
			sum = (sum + valuesIt.next().get());
		}
		
		context.write(key, new DoubleWritable(Math.round(sum/count * 1e1) / 1e1));
	}	
}