package org.myorg;

import java.util.*;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ToolRunner;
import org.myorg.Moralization.*;
import org.myorg.JunctionTreeConstruction.*;

public class Runner {
	static String basePath = "";
	public static void main(String[] args) throws Exception {

		System.out.println("Moralizing....");
		JobConf conf = new JobConf(Moralization.class);
		conf.setJobName("moralize");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MoralizationMap1.class);
		conf.setReducerClass(MoralizationReduce1.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(
				basePath + "moralize/input"));
		FileOutputFormat.setOutputPath(conf, new Path(
				basePath + "moralize/inter"));
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(basePath + "moralize/input")))
			fs.delete(new Path(basePath + "moralize/input"), true);
		fs.copyFromLocalFile(new Path(args[0]),
				new Path(basePath + "moralize/input"));
		if (fs.exists(new Path(basePath + "moralize/inter")))
			fs.delete(new Path(basePath + "moralize/inter"), true);
		JobClient.runJob(conf);

		// end of job1.

		fs.copyFromLocalFile(new Path(args[0]),
				new Path(basePath + "moralize/inter"));

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
				basePath + "moralize/inter"));
		FileOutputFormat.setOutputPath(conf2, new Path(
				basePath + "moralize/output"));
		if (fs.exists(new Path(basePath + "moralize/output")))
			fs.delete(new Path(basePath + "moralize/output"), true);

		JobClient.runJob(conf2);

		// End of moralization

		// Triangulation
		System.out.println("Triangulation...");

       FileStatus[] fss = fs.listStatus(new Path(basePath + "moralize/output/"),new PathFilter() {

             @Override

             public boolean accept(Path path) {

               return path.getName().startsWith("part");

             }

           });

       HashMap<String, HashSet<String>> adj = new HashMap<String, HashSet<String>>();

       String[] cliq = null;

       for(FileStatus status : fss){

           FSDataInputStream fsdis = fs.open(status.getPath());

           Scanner read = new Scanner(fsdis);

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

       }


		String targ = null;
		int min = Integer.MAX_VALUE;

		Iterator<String> adj_iterator = adj.keySet().iterator();
		while (adj_iterator.hasNext()) {
			String t = adj_iterator.next();
			Iterator<String> iti = adj.get(t).iterator();
			while (iti.hasNext())
				t += "\t" + iti.next();
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
				targ = cliq[0];
				for (int i = 1; i < cliq.length; i++)
					targ += "\t" + cliq[i];
				min = count;
			}
		}

		ArrayList<HashSet<String>> Cliques = new ArrayList<HashSet<String>>();
		Boolean done = false;
		HashSet<String> visited = new HashSet<String>();

		while (!done) {

			System.out.println(targ);

			cliq = targ.split("\\s+");
			visited.add(cliq[0]);
			HashSet<String> hs = new HashSet<String>();
			for (int i = 0; i < cliq.length; i++)
				hs.add(cliq[i]);
			System.out.println(hs.toString());
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

			min = Integer.MAX_VALUE;
			done = true;
			adj_iterator = adj.keySet().iterator();
			while (adj_iterator.hasNext()) {
				String t = adj_iterator.next();
				Iterator<String> itit = adj.get(t).iterator();
				while (itit.hasNext())
					t += "\t" + itit.next();
				cliq = t.split("\\s+");
				int count = 0;
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

		}
		
		// end of triangulation.
		FSDataOutputStream fsdos = fs.create(new Path(
				basePath + "maximal_clique/output/part-00000"));
		for (int i = 0; i < Cliques.size(); i++) {
			Iterator<String> it = Cliques.get(i).iterator();
			while (it.hasNext())
				fsdos.writeBytes(it.next() + "\t");
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
		conf6.setReducerClass(JT_Reduce1.class);

		conf6.setInputFormat(TextInputFormat.class);
		conf6.setOutputFormat(TextOutputFormat.class);

		if (fs.exists(new Path(basePath + "junction_tree/output")))
			fs.delete(new Path(basePath + "junction_tree/output"), true);
		FileInputFormat.setInputPaths(conf6, new Path(
				basePath + "maximal_clique/output"));
		FileOutputFormat.setOutputPath(conf6, new Path(
				basePath + "junction_tree/output"));

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

		if (fs.exists(new Path(basePath + "junction_tree/output2")))
			fs.delete(new Path(basePath + "junction_tree/output2"), true);
		FileInputFormat.setInputPaths(conf7, new Path(
				basePath + "junction_tree/output"));
		FileOutputFormat.setOutputPath(conf7, new Path(
				basePath + "junction_tree/output2"));

		JobClient.runJob(conf7);

		// Sorting the separation sets.
		JobConf conf8 = new JobConf(JunctionTreeConstruction.class);
		conf8.setJobName("Junction tree construction - part3");

		conf8.setOutputKeyClass(Text.class);
		conf8.setOutputValueClass(Text.class);

		conf8.setMapperClass(JT_Map3.class);
		conf8.setReducerClass(JT_Reduce3.class);
		conf8.setInputFormat(TextInputFormat.class);
		conf8.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf8, new Path(
				basePath + "junction_tree/output2"));
		FileOutputFormat.setOutputPath(conf8, new Path(
				basePath + "junction_tree/sorted_output"));
		if (fs.exists(new Path(basePath + "junction_tree/sorted_output")))
			fs.delete(new Path(basePath + "junction_tree/sorted_output"),
					true);

		JobClient.runJob(conf8);
		if (fs.exists(new Path(basePath + "junction_tree/final_output")))
			fs.delete(new Path(basePath + "junction_tree/final_output"),
					true);
		String path[] = { basePath + "junction_tree/sorted_output",
				basePath + "junction_tree/final_output" };
		
		
		// Minimum Spanning tree.
		ToolRunner.run(new Configuration(), new MST(), path);
		fs.copyToLocalFile(new Path(
				basePath + "junction_tree/final_output/part-r-00000"),
				new Path("../../output/junction_tree"));
	}

}
