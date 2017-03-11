package com.javacodegeeks.examples.wordcount;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The entry point for the WordCount example,
 * which setup the Hadoop job with Map and Reduce Class
 * 
 * @author Raman
 */
public class MostReviews extends Configured implements Tool{
	
	/**
	 * Main function which calls the run method and passes the args using ToolRunner
	 * @param args Two arguments input and output file paths
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new MostReviews(), args);
		System.exit(exitCode);
	}
 
	/**
	 * Run method which schedules the Hadoop Job
	 * @param args Arguments passed in main function
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.printf("Usage: %s needs two arguments <input> <output> files\n",
					getClass().getSimpleName());
			return -1;
		}
	
		//Initialize the Hadoop job and set the jar as well as the name of the Job
		Job job = new Job();
		Job job2 = new Job();
		
		job.setJarByClass(MostReviews.class);
		job.setJobName("MostReviews");
		job2.setJarByClass(MostReviews.class);
		job2.setJobName("MovieRatings");
		
		//Add input and output file paths to job based on the arguments passed
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		//Set the MapClass and ReduceClass in the job
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job2.setMapperClass(Map2Class.class);
		job2.setReducerClass(Reduce2Class.class);
	
		//Wait for the job to complete and print if the job was successful or not
		int returnValue = job.waitForCompletion(true) ? 0:1;
		int returnValue2 = job2.waitForCompletion(true) ? 0:1;
		
		if(job.isSuccessful() && job2.isSuccessful()) {
			findtop10("movie_titles.txt", "output2.txt/part-r-00000", "output.txt/part-r-00000");
			System.out.println("Job was successful");
		} else {
			System.out.println("Job was not successful");			
		}
		
		return returnValue;
	}
	
	public static void findtop10(String movieinput, String mapinput,String userinput)
	{
		BufferedReader br = null;
		FileReader fr = null;
		Comparator<String[]> comparator = new StringLengthComparator();
		Comparator<String[]> comparator2 = new RatingComparator();
	    PriorityQueue<String[]> queue = new PriorityQueue<String[]>(10, comparator);
	    PriorityQueue<String[]> userqueue = new PriorityQueue<String[]>(10, comparator2);
	    HashMap<String, String> map = new HashMap<>();

		try {

			fr = new FileReader(movieinput);
			br = new BufferedReader(fr);

			String sCurrentLine;

			br = new BufferedReader(new FileReader(movieinput));

			while ((sCurrentLine = br.readLine()) != null) {
				StringTokenizer st = new StringTokenizer(sCurrentLine,",");
				String key = st.nextToken();
				st.nextToken();
				String value = st.nextToken();
				map.put(key, value);
			}
			
			fr = new FileReader(mapinput);
			br = new BufferedReader(fr);

			br = new BufferedReader(new FileReader(mapinput));

			while ((sCurrentLine = br.readLine()) != null) {
				StringTokenizer st = new StringTokenizer(sCurrentLine,"\t");
				String movierating[] = new String[2];
				movierating[0] = st.nextToken();
				movierating[1] = st.nextToken();
				queue.add(movierating);
			}
			
			fr = new FileReader(userinput);
			br = new BufferedReader(fr);

			br = new BufferedReader(new FileReader(userinput));
			while ((sCurrentLine = br.readLine()) != null) {
				StringTokenizer st = new StringTokenizer(sCurrentLine," \t");
				String users[] = new String[2];
				users[0] = st.nextToken();
				users[1] = st.nextToken();
				userqueue.add(users);
			}
			BufferedWriter bw = null, bw2 = null;
			FileWriter fw = null, fw2 = null;
			
			fw = new FileWriter("top10movie.txt");
			bw = new BufferedWriter(fw);
			bw.write(map.get(queue.poll()[0]) + "\n");
			bw.write(map.get(queue.poll()[0]) + "\n");
			bw.write(map.get(queue.poll()[0]) + "\n");
			bw.write(map.get(queue.poll()[0]) + "\n");
			bw.write(map.get(queue.poll()[0]) + "\n");
			bw.write(map.get(queue.poll()[0]) + "\n");
			bw.write(map.get(queue.poll()[0]) + "\n");
			bw.write(map.get(queue.poll()[0]) + "\n");
			bw.write(map.get(queue.poll()[0]) + "\n");
			bw.write(map.get(queue.poll()[0]) + "\n");
			bw.close();
			fw.close();
			
			fw2 = new FileWriter("top10user.txt");
			bw2 = new BufferedWriter(fw2);
			bw2.write(userqueue.poll()[0] + "\n");
			bw2.write(userqueue.poll()[0] + "\n");
			bw2.write(userqueue.poll()[0] + "\n");
			bw2.write(userqueue.poll()[0] + "\n");
			bw2.write(userqueue.poll()[0] + "\n");
			bw2.write(userqueue.poll()[0] + "\n");
			bw2.write(userqueue.poll()[0] + "\n");
			bw2.write(userqueue.poll()[0] + "\n");
			bw2.write(userqueue.poll()[0] + "\n");
			bw2.write(userqueue.poll()[0] + "\n");
			bw2.close();
			fw2.close();

		} catch (IOException e) {

			e.printStackTrace();

		} finally {

			try {

				if (br != null)
					br.close();

				if (fr != null)
					fr.close();

			} catch (IOException ex) {

				ex.printStackTrace();

			}

		}
	}
}


