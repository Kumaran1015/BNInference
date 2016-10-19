package org.myorg;


import java.io.IOException;


import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

import java.util.HashMap;

import java.util.ArrayList;

import java.util.Iterator;

import java.util.Scanner;


import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.hadoop.mapreduce.Mapper;

public class PTC {

	static String basePath = "";
	public static class DeserializeMap extends Mapper<Text, Node, Text, Node> {

		@Override
		public void map(Text key, Node value, Context context)
				throws IOException, InterruptedException {

			System.out.println(key.toString());
			Configuration conf = context.getConfiguration();
			String[] nodes = conf.get("clique").split("\\s+");
			boolean include = true;
			HashMap<String, Integer> hmap = new HashMap<String, Integer>();
			for (int i = 0; i < nodes.length; i++) {
				System.out.println(nodes[i]);
				hmap.put(nodes[i], i);
			}
			System.out.println("me : " + value.name);
			if (hmap.containsKey(value.name)) {
				for (int i = 0; i < value.parents.length; i++) {
					System.out.println("parents " + value.parents[i][0]);
					if (!hmap.containsKey(value.parents[i][0])) {
						include = false;
						break;
					}
				}
			} else {
				include = false;
			}
			System.out.println("Map done : " + include);
			if (include == false) {
				System.out.println("include is false");
				context.write(new Text("mismatch"), value);
			} else {
				System.out.println("include is true");
				context.write(new Text(conf.get("clique")), value);
			}

		}
	}

	public static class DeserializeReduce extends
			Reducer<Text, Node, Text, Node> {

		public int k = 0;
		public static enum Counter {
		    CONVERGED
		  }
		private void recurse(HashMap<HashMap<String, String>, Double> hmap,
				HashMap<String, String> map, int index, ArrayList<Node> nodes) {
			if (index == nodes.size()) {

				HashMap<String, String> dummy = (HashMap<String, String>) map.clone();
				hmap.put(dummy, 1.0);

			} else {
				Node cur = nodes.get(index);
				for (String value : cur.values) {
					map.put(cur.name, value);

					recurse(hmap, map, index + 1, nodes);
				}
			}
		}

	

		private void recurse_inner(
				HashMap<HashMap<String, String>, Double> hmap_inner,
				HashMap<String, String> map_inner, int index,
				ArrayList<Node> nodes_inner, double[] prob) {

			if (index == nodes_inner.size()) {
				HashMap<String, String> dummy = (HashMap<String, String>) map_inner.clone();
				hmap_inner.put(dummy, prob[k++]);
			} else {
				Node cur = nodes_inner.get(index);
				for (String value : cur.values) {
					map_inner.put(cur.name, value);
					recurse_inner(hmap_inner, map_inner, index + 1,
							nodes_inner, prob);
				}
			}

		}

		public void reduce(Text key, Iterable<Node> values, Context context)
				throws IOException, InterruptedException {
			
			

			if (key.toString().equals("mismatch")) {
				for (Node emit_node : values) {
					context.write(new Text("1"), emit_node);
				}
			} else {
				context.getCounter(Counter.CONVERGED).increment(1);
				ArrayList<Node> nodes = new ArrayList<Node>();
				HashMap<String, Node> node_map = new HashMap<String, Node>();
				System.out.println(key.toString());
				HashMap<String, String> map = new HashMap<String, String>();
				HashMap<HashMap<String, String>, Double> hmap = new HashMap<HashMap<String, String>, Double>();
				Configuration conf = context.getConfiguration();
				FileSystem fs3 = FileSystem.get(conf);
				FSDataOutputStream fw = fs3.append(new Path(
						basePath + "FYP/initial-potentials"));

				fw.writeBytes(key.toString() + "\n");
				HashMap<String, String> map_inner = new HashMap<String, String>();
				HashMap<HashMap<String, String>, Double> hmap_inner = new HashMap<HashMap<String, String>, Double>();
				ArrayList<Node> nodes_inner = new ArrayList<Node>();
				ArrayList<Node> node_ip = new ArrayList<Node>();

				for (Node log : values) {
					Node ref = new Node();
					ref.name = log.name;
					ref.CPD = log.CPD;
					ref.parents = log.parents;
					ref.values = log.values;
					node_ip.add(ref);

					boolean contains = false;
					for (Node cur : nodes) {
						if (cur.name.equals(log.name)) {
							contains = true;
							break;
						}
					}
					if (contains == false) {
						nodes.add(log);
						node_map.put(log.name, log);
					}
					for (String[] parent : log.parents) {
						boolean isPresent = false;
						for (Node cur : nodes) {
							if (cur.name.equals(parent[0])) {
								isPresent = true;
								break;
							}
						}
						if (isPresent == false) {
							Node temp = new Node();
							String val[] = new String[parent.length - 1];
							for (int i = 1; i < parent.length; i++)
								val[i - 1] = parent[i];
							temp.set(parent[0], val);
							nodes.add(temp);
							node_map.put(temp.name, temp);
						}
					}
				}
				System.out.print("\n");
				recurse(hmap, map, 0, nodes);
				System.out.println("\nHmap : " + hmap.toString());
				for (int i = 0; i < nodes.size(); i++)
					fw.writeBytes(nodes.get(i).name + "\t");
				fw.writeBytes("\n");
				for (Node log : node_ip) {
					System.out.println("came here : " + log.name + " "
							+ hmap_inner.toString());
					
					for (String parent[] : log.parents) {
						nodes_inner.add(node_map.get(parent[0]));
					}
					nodes_inner.add(log);
					double[] prob = new double[log.CPD.length
							* log.CPD[0].length];
					k = 0;
					for (int i = 0; i < log.CPD.length; i++) {
						for (int j = 0; j < log.CPD[i].length; j++)
							prob[k++] = log.CPD[i][j];
					}
					k = 0;
					recurse_inner(hmap_inner, map_inner, 0, nodes_inner, prob);
					System.out.println("Inner hmap : " + hmap_inner.toString());
					Iterator<HashMap<String, String>> iter = hmap_inner
							.keySet().iterator();
					while (iter.hasNext()) {
						HashMap<String, String> temp = iter.next();
						Iterator<HashMap<String, String>> iter2 = hmap.keySet()
								.iterator();
						while (iter2.hasNext()) {
							HashMap<String, String> temp2 = iter2.next();
							Iterator<String> iter3 = temp.keySet().iterator();
							boolean match = true;
							while (iter3.hasNext()) {
								String search = iter3.next();
								if (!(temp2.containsKey(search) && temp2.get(
										search).equals(temp.get(search))))
									match = false;
							}
							if (match == true) {
								hmap.put(temp2,
										hmap.get(temp2) * hmap_inner.get(temp));
							}
						}
					}
					hmap_inner.clear();
					nodes_inner.clear();
					map_inner.clear();
				}
				CliqueNode cnode = new CliqueNode();
				String[] clique_nodes = key.toString().split("\\s+");
				String clique_name = "";
				for (int i = 0; i < clique_nodes.length - 1; i++)
					clique_name += clique_nodes[i] + ":";
				clique_name += clique_nodes[clique_nodes.length - 1];
				cnode.set(clique_name);
				Node[] dependencies = new Node[nodes.size()];
				for (int i = 0; i < nodes.size(); i++)
					dependencies[i] = nodes.get(i);
				
				cnode.setdependencies(dependencies);
				/*for (int i = 0; i < cnode.dependencies.length; i++)
					size *= cnode.dependencies[i].length - 1;
				double CPD[] = new double[size];
				map.clear();
				k = 0;
				recursiveGet(hmap, map, 0, nodes, CPD);
				cnode.setCPD(CPD);*/
				cnode.setCPD(hmap);
				System.out.println("Result : " + hmap.toString() + "\n");
				
				fw.writeBytes(hmap.toString() + "\n\n");
				String file_name = "";
				for (int i = 0; i < clique_nodes.length - 1; i++)
					file_name += clique_nodes[i] + "_";
				file_name += clique_nodes[clique_nodes.length - 1];
				if(fs3.exists(new Path(basePath + "FYP/" + file_name)))
					fs3.delete(new Path(basePath + "FYP/" + file_name),true);
				Path path = new Path(basePath + "FYP/" + file_name
						+ "/file.seq");
				SequenceFile.Writer nodeWriter = SequenceFile.createWriter(fs3,
						conf, path, Text.class, CliqueNode.class);
				nodeWriter.append(new Text(cnode.name), cnode);
				nodeWriter.close();

				fw.close();
			}

		}

	}

	public static void main(String[] args) throws Exception {
		int iteration = 1;
		// File input = new
		// File("/user/training/maximal_clique/output/part-00000");

		Configuration con = new Configuration();
		FileSystem fs4 = FileSystem.get(con);
		if (fs4.exists(new Path(basePath + "FYP/initial-potentials")))
			fs4.delete(new Path(basePath + "FYP/initial-potentials"), true);
		fs4.create(new Path(basePath + "FYP/initial-potentials"), true);
		fs4.close();
		FileSystem fs2 = FileSystem.get(con);
		FSDataInputStream input = fs2.open(new Path(
				basePath + "maximal_clique/output/part-00000"));
		Scanner read = new Scanner(input);
		while (read.hasNext()) {
			Configuration config = new Configuration();
			String get = read.nextLine();
			config.set("clique", get);
			Job job = new Job(config);

			job.setJobName("Potential table computaiton iteration" + iteration);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Node.class);

			job.setMapperClass(DeserializeMap.class);
			// job.setCombinerClass(DeserializeReduce.class);
			job.setReducerClass(DeserializeReduce.class);
			job.setJarByClass(PTC.class);
			
			FileInputFormat.addInputPath(job, new Path(
					basePath + "FYP/BN-out" + iteration));
			FileOutputFormat.setOutputPath(job, new Path(
					basePath + "FYP/BN-out" + (iteration + 1)));
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileSystem fs = FileSystem.get(config);
			if (fs.exists(new Path(basePath + "FYP/BN-out"
					+ (iteration + 1))))
				fs.delete(new Path(basePath + "FYP/BN-out"
						+ (iteration + 1)), true);
			job.waitForCompletion(true);
			long counter = job.getCounters().findCounter(DeserializeReduce.Counter.CONVERGED).getValue();
			System.out.println("Counter : "+counter);
			if(counter == 0)
			{
				String file_name = "";
				String[] clique_nodes = get.split("\\s+");
				for (int i = 0; i < clique_nodes.length - 1; i++)
					file_name += clique_nodes[i] + "_";
				file_name += clique_nodes[clique_nodes.length - 1];
				if(fs2.exists(new Path(basePath + "FYP/" + file_name)))
					fs2.delete(new Path(basePath + "FYP/" + file_name),true);
				Path path = new Path(basePath + "FYP/" + file_name
						+ "/file.seq");
				SequenceFile.Writer nodeWriter = SequenceFile.createWriter(fs2,
						config, path, Text.class, CliqueNode.class);
				CliqueNode cnode = new CliqueNode();
				String clique_name = "";
				for (int i = 0; i < clique_nodes.length - 1; i++)
					clique_name += clique_nodes[i] + ":";
				clique_name += clique_nodes[clique_nodes.length - 1];
				cnode.set(clique_name);
				nodeWriter.append(new Text(cnode.name), cnode);
				nodeWriter.close();
			}
			iteration++;
		}
		fs2.close();
	}
}
