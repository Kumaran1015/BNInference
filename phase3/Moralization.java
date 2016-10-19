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

public class Moralization {

	public static class MoralizationMap1 extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		/*
		 * Input : a b c Output : <b a> , <c a> reverse emit the node and their
		 * children.
		 */
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] nodes = line.split("\\s+");
			for (int i = 1; i < nodes.length; i++) {
				output.collect(new Text(nodes[i]), new Text(nodes[0]));

			}

		}
	}

	public static class MoralizationReduce1 extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		/*
		 * Input : a <iterator over parents of a> Output : a <parents of a say
		 * p1 , p2 , p3 ,...> <p1 p2> <p2 p1> <p1 p3> <p3 p1> and like wise.
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
		 * java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 */
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String temp = "";
			String t;
			ArrayList<String> arr = new ArrayList<String>();
			while (values.hasNext()) {
				t = values.next().toString();
				temp += t + "\t";
				arr.add(t);
			}
			output.collect(key, new Text(temp));
			int len = arr.size();
			for (int i = 0; i < len; i++) {
				for (int j = i + 1; j < len; j++) {
					output.collect(new Text(arr.get(i)), new Text(arr.get(j)));
					output.collect(new Text(arr.get(j)), new Text(arr.get(i)));
				}
			}

		}

	}

	public static class Map2 extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		/*
		 * Input : a b c d Output : <a b> , <a c> , <a d> (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
		 * java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 */
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] nodes = line.split("\\t");
			for (int i = 1; i < nodes.length; i++) {
				output.collect(new Text(nodes[0]), new Text(nodes[i]));
				System.out.println(nodes[0] + "    " + nodes[i]);
			}

		}
	}

	public static class Reduce2 extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		/*
		 * Input : a <iterator over adjacencies of a(say b,c,d..)> Output : a b
		 * c d (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
		 * java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 */
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String temp = "";
			String t;
			while (values.hasNext()) {
				t = values.next().toString();
				temp += t + "\t";
			}
			output.collect(key, new Text(temp));

		}

	}
}
