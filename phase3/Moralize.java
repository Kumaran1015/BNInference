package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ToolRunner;
import org.myorg.Moralization.*;
import org.myorg.JunctionTreeConstruction.*;

public class Moralize {

	public static class Merge_Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		// private final static IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String nodes[] = line.split("\\s+");

			for (int i = 1; i < nodes.length; i++)
				output.collect(new Text(nodes[0]), new Text(nodes[i]));
		}
	}

	public static class Merge_Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// System.out.println("KEY : "+key.toString());
			HashSet<String> bucket = new HashSet<String>();
			while (values.hasNext()) {
				bucket.add(values.next().toString());
			}
			String t = "";
			Iterator<String> it = bucket.iterator();
			while (it.hasNext())
				t += it.next() + "\t";
			output.collect(key, new Text(t));
		}
	}

	public static void main(String[] args) throws Exception {

		System.out.println("Moralizing....");
		JobConf conf = new JobConf(Moralization.class);
		conf.setJobName("moralize");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MoralizationMap1.class);
		// conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(MoralizationReduce1.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(
				"/user/training/moralize/input"));
		FileOutputFormat.setOutputPath(conf, new Path(
				"/user/training/moralize/inter"));
		FileSystem fs = FileSystem.get(conf);
		fs.copyFromLocalFile(new Path(args[0]),
				new Path("/user/training/moralize/input"));
		if (fs.exists(new Path("/user/training/moralize/inter")))
			fs.delete(new Path("/user/training/moralize/inter"), true);
		JobClient.runJob(conf);
		
		//end of job1.
		
		
		fs.copyFromLocalFile(new Path(args[0]),
				new Path("/user/training/moralize/inter"));

		JobConf conf2 = new JobConf(Moralization.class);
		conf2.setJobName("moralize2");

		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(Text.class);

		conf2.setMapperClass(Map2.class);
		// conf2.setCombinerClass(Reduce2.class);
		conf2.setReducerClass(Reduce2.class);

		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf2, new Path(
				"/user/training/moralize/inter"));
		FileOutputFormat.setOutputPath(conf2, new Path(
				"/user/training/moralize/output"));
		if (fs.exists(new Path("/user/training/moralize/output")))
			fs.delete(new Path("/user/training/moralize/output"), true);

		JobClient.runJob(conf2);

		//End of moralization
		
		
		
		
		// Triangulation
		System.out.println("Triangulation...");
		FSDataInputStream fsdis = fs.open(new Path(
				"/user/training/moralize/output/part-00000"));
		HashMap<String, HashSet<String>> adj = new HashMap<String, HashSet<String>>();

		Scanner read = new Scanner(fsdis);
		String[] cliq = null;
		while (read.hasNext()) {
			String t = read.nextLine();
			cliq = t.split("\\s+");
			String ind = cliq[0];
			HashSet<String> temp = new HashSet<String>();
			for (int i = 1; i < cliq.length; i++)
				temp.add(cliq[i]);
			adj.put(ind, temp);
		}
		fsdis.close();
		fsdis = fs.open(new Path("/user/training/moralize/output/part-00000"));
		read = new Scanner(fsdis);
		String targ = null;
		int min = 1000;
		while (read.hasNext()) {
			String t = read.nextLine();
			cliq = t.split("\\s+");
			int count = 0;

			for (int i = 1; i < cliq.length; i++) {
				for (int j = i + 1; j < cliq.length; j++) {
					HashSet<String> hs = (HashSet<String>) adj.get(cliq[i]);
					if (hs.contains(cliq[j]) == false) {
						count++;
					}
				}

			}
			if (count <= min) {
				targ = t;
				min = count;
			}

		}

		ArrayList<HashSet<String>> Cliques = new ArrayList<HashSet<String>>();
		Boolean done = false;
		HashSet<String> visited = new HashSet<String>();
		FSDataOutputStream fsdos;

		if (fs.exists(new Path("/user/training/joinadj/output")))
			fs.delete(new Path("/user/training/joinadj/output"), true);
		while (!done) {
			System.out.println("target : " + targ);
			cliq = targ.split("\\s+");
			visited.add(cliq[0]);
			HashSet hs = new HashSet();
			for (int i = 0; i < cliq.length; i++)
				hs.add(cliq[i]);

			// change adjacency accordingly.
			for (int i = 1; i < cliq.length; i++)
				for (int j = 1; j < cliq.length; j++)
					if (i != j) {
						HashSet<String> t = adj.get(cliq[i]);
						if (t.contains(cliq[j]) == false) {
							t.add(cliq[j]);
							adj.put(cliq[i], t);
						}
					}

			boolean contain = false;
			for (int i = 0; i < Cliques.size(); i++)
				if (Cliques.get(i).containsAll(hs))
					contain = true;
			if (contain == false)
				Cliques.add(hs);

			if (fs.exists(new Path("/user/training/joinadj/output"))) {
				fsdis = fs.open(new Path(
						"/user/training/joinadj/output/part-00000"));

			} else {
				fsdis = fs.open(new Path(
						"/user/training/moralize/output/part-00000"));

			}
			fsdos = fs.create(new Path(
					"/user/training/triangulation/input/file01"));
			read = new Scanner(fsdis);
			while (read.hasNext()) {
				fsdos.writeBytes(read.nextLine() + "\n");
			}

			fsdis.close();
			fsdos.close();

			fsdos = fs.create(new Path(
					"/user/training/triangulation/input/file02"));
			for (int i = 1; i < cliq.length; i++) {
				for (int j = i + 1; j < cliq.length; j++) {
					String t = cliq[i].toString() + "\t" + cliq[j].toString();
					fsdos.writeBytes(t + "\n");
					t = cliq[j].toString() + "\t" + cliq[i].toString();
					fsdos.writeBytes(t + "\n");
				}
			}
			fsdos.close();
			// join adjacencies ---- job.

			JobConf conf4 = new JobConf(Moralize.class);
			conf4.setJobName("Merging Adjacencies");

			conf4.setMapOutputKeyClass(Text.class);
			conf4.setMapOutputValueClass(Text.class);
			conf4.setOutputKeyClass(Text.class);
			conf4.setOutputValueClass(Text.class);

			conf4.setMapperClass(Merge_Map.class);

			conf4.setReducerClass(Merge_Reduce.class);

			conf4.setInputFormat(TextInputFormat.class);
			conf4.setOutputFormat(TextOutputFormat.class);
			if (fs.exists(new Path("/user/training/joinadj/output")))
				fs.delete(new Path("/user/training/joinadj/output"), true);
			FileInputFormat.setInputPaths(conf4, new Path(
					"/user/training/triangulation/input"));
			FileOutputFormat.setOutputPath(conf4, new Path(
					"/user/training/joinadj/output"));

			JobClient.runJob(conf4);

			fsdis = fs
					.open(new Path("/user/training/joinadj/output/part-00000"));
			read = new Scanner(fsdis);
			min = 1000;
			done = true;
			while (read.hasNext()) {
				String t = read.nextLine();
				cliq = t.split("\\s+");
				int count = 0;
				String curCliq = cliq[0];
				if (visited.contains(cliq[0]))
					continue;
				done = false;
				for (int i = 1; i < cliq.length; i++) {
					for (int j = i + 1; j < cliq.length; j++) {
						HashSet<String> has = (HashSet<String>) adj
								.get(cliq[i]);
						if (visited.contains(cliq[i]) == false
								&& visited.contains(cliq[j]) == false
								&& has.contains(cliq[j]) == false) {
							count++;
						}
					}

				}
				if (count <= min) {
					targ = cliq[0];
					for (int i = 1; i < cliq.length; i++)
						if (visited.contains(cliq[i]) == false)
							targ += "\t" + cliq[i];
					min = count;
				}

			}

			/*
			 * Scanner sc = new Scanner(System.in); sc.next();
			 */

		}
		// end of triangulation.
		fsdos = fs.create(new Path(
				"/user/training/maximal_clique/output/part-00000"));
		for (int i = 0; i < Cliques.size(); i++) {
			Iterator<String> it = Cliques.get(i).iterator();
			while (it.hasNext())
				fsdos.writeBytes(it.next() + "\t");
			// System.out.print(it.next());
			// System.out.println("\n");
			fsdos.writeBytes("\n");
		}
		fsdos.close();
		// Generated maximal cliques.

		
		
		
		// start of junction tree construction.
		System.out.println("Junction Tree Construction....");

		JobConf conf6 = new JobConf(JunctionTreeConstruction.class);
		conf6.setJobName("Junction tree construction - part1");

		conf6.setOutputKeyClass(Text.class);
		conf6.setOutputValueClass(Text.class);

		conf6.setMapperClass(JT_Map1.class);
		// conf6.setCombinerClass(JT_Reduce1.class);
		conf6.setReducerClass(JT_Reduce1.class);

		conf6.setInputFormat(TextInputFormat.class);
		conf6.setOutputFormat(TextOutputFormat.class);

		if (fs.exists(new Path("/user/training/junction_tree/output")))
			fs.delete(new Path("/user/training/junction_tree/output"), true);
		FileInputFormat.setInputPaths(conf6, new Path(
				"/user/training/maximal_clique/output"));
		FileOutputFormat.setOutputPath(conf6, new Path(
				"/user/training/junction_tree/output"));

		JobClient.runJob(conf6);

		// Forming the separation sets.

		JobConf conf7 = new JobConf(JunctionTreeConstruction.class);
		conf7.setJobName("Junction tree construction - part2");

		conf7.setOutputKeyClass(Text.class);
		conf7.setOutputValueClass(Text.class);

		conf7.setMapperClass(JT_Map2.class);
		conf7.setReducerClass(JT_Reduce2.class);

		conf7.setInputFormat(TextInputFormat.class);
		conf7.setOutputFormat(TextOutputFormat.class);

		if (fs.exists(new Path("/user/training/junction_tree/output2")))
			fs.delete(new Path("/user/training/junction_tree/output2"), true);
		FileInputFormat.setInputPaths(conf7, new Path(
				"/user/training/junction_tree/output"));
		FileOutputFormat.setOutputPath(conf7, new Path(
				"/user/training/junction_tree/output2"));

		JobClient.runJob(conf7);

		// Sorting the separation sets.
		JobConf conf8 = new JobConf(JunctionTreeConstruction.class);
		conf8.setJobName("Junction tree construction - part3");

		conf8.setOutputKeyClass(Text.class);
		conf8.setOutputValueClass(Text.class);

		conf8.setMapperClass(JT_Map3.class);
		conf8.setReducerClass(JT_Reduce3.class);
		// conf8.setOutputKeyComparatorClass(ReverseComparator.class);
		conf8.setInputFormat(TextInputFormat.class);
		conf8.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf8, new Path(
				"/user/training/junction_tree/output2"));
		FileOutputFormat.setOutputPath(conf8, new Path(
				"/user/training/junction_tree/sorted_output"));
		if (fs.exists(new Path("/user/training/junction_tree/sorted_output")))
			fs.delete(new Path("/user/training/junction_tree/sorted_output"),
					true);

		JobClient.runJob(conf8);
		if (fs.exists(new Path("/user/training/junction_tree/final_output")))
			fs.delete(new Path("/user/training/junction_tree/final_output"),
					true);
		String path[] = { "/user/training/junction_tree/sorted_output",
				"/user/training/junction_tree/final_output" };
		// Spanning tree.
		int res = ToolRunner.run(new Configuration(), new MST(), path);
		fs.copyToLocalFile(new Path(
				"/user/training/junction_tree/final_output/part-r-00000"),
				new Path("/home/training/output/junction_tree"));
	}

}
