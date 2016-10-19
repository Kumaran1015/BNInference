package org.myorg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class JunctionTreeConstruction {
	public static class JT_Map1 extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		/*
		 * Input : a b c Output : <a,a:b:c> <b,a:b:c> <c,a:b:c> (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
		 * java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 */
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String line = value.toString();
			String cliq = "";
			String[] nodes = line.split("\\s+");

			for (int i = 0; i < nodes.length - 1; i++) {
				cliq += nodes[i] + ':';

			}
			cliq += nodes[nodes.length - 1];

			Text word = new Text(cliq);
			for (int i = 0; i < nodes.length; i++) {

				output.collect(new Text(nodes[i]), word);

			}

		}

	}

	public static class JT_Reduce1 extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		/*
		 * Input : <b , <a:b:c,b:c:e>> Output : <a:b:c,b:c:e , b> (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
		 * java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 */
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String t;
			ArrayList<String> arr = new ArrayList<String>();

			while (values.hasNext()) {
				t = values.next().toString();
				arr.add(t);
			}

			int len = arr.size();
			for (int i = 0; i < len; i++) {
				for (int j = i + 1; j < len; j++) {
					String temp = "";
					if (arr.get(i).compareTo(arr.get(j)) < 0)
						temp = arr.get(i) + "," + arr.get(j);
					else
						temp = arr.get(j) + "," + arr.get(i);
					output.collect(new Text(temp), key);

				}
			}

		}

	}

	public static class JT_Map2 extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		/*
		 * Input : a:b:c,b:c:e b Output: <"a:b:c,b:c:e" , b> (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
		 * java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 */
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] nodes = line.split("\\s+");
			output.collect(new Text(nodes[0]), new Text(nodes[1]));

		}

	}

	public static class JT_Reduce2 extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		/*
		 * Input : <"a:b:c,b:c:e" , <b,c>> Output : <"a:b:c,b:c:e" , "b:c:">
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
		 * java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 */
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String t;

			String sepset = "";

			while (values.hasNext()) {

				t = values.next().toString();
				System.out.println(t);
				sepset += t + ":";
			}

			Text emit_value = new Text(sepset);
			output.collect(key, emit_value);

		}

	}

	public static class JT_Map3 extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		/*
		 * Input : <"a:b:c,b:c:e" , "b:c:"> Output : <-2 , "a:b:c,b:c:e">
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
		 * java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 */
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String line = value.toString();
			String[] clusters = line.split("\\s+");
			String[] nodes = clusters[1].split(":");
			Integer l = -1 * nodes.length;
			output.collect(new Text(l.toString()), new Text(clusters[0]));

		}

	}

	public static class JT_Reduce3 extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		/*
		 * Identity reducer. (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
		 * java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 */
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String t;
			while (values.hasNext()) {
				t = values.next().toString();
				System.out.println(t);
				output.collect(key, new Text(t));
			}

		}

	}

}
