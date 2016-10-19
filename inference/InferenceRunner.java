package org.myorg;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.myorg.OutputBuilder.OutputBuilderMap;
import org.myorg.OutputBuilder.OutputBuilderReduce;
import org.myorg.EvidenceCollection.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.myorg.EvidenceDistribution.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

class JTNode implements Comparable<JTNode>{
	String name;
	ArrayList<JTNode> children = new ArrayList<JTNode>();
	void set(String name){
		this.name = name;
	}
	void addChild(JTNode child){
		this.children.add(child);
	}
	@Override
	public int compareTo(JTNode o) {
		for(int i=0;i<this.children.size();i++){
			if(this.children.get(i).name.equals(o.name))
				return -1;
		}
		return 1;
	}
}

public class InferenceRunner {
	static String basePath = "";
	Configuration conf;
	FileSystem fs;
	HashMap<String,Boolean> visited = new HashMap<String,Boolean>();
	HashMap<String,String> evidence = new HashMap<String,String>();
	InferenceRunner() throws IOException, InterruptedException, ClassNotFoundException{
		conf = new Configuration();
		fs = FileSystem.get(conf);
		FSDataInputStream fsdis = fs.open(new Path(basePath + "junction_tree/final_output/part-r-00000")); 
		System.out.println("Enter the student ID...");
		Scanner get = new Scanner(System.in);
		String studentId = get.nextLine();
		String json = getAssessmentByStudentID(studentId);
		evidence = parse(json);
		ArrayList<JTNode> jtnodes = new ArrayList<JTNode>();
		HashMap<String,Integer> hmap = new HashMap<String,Integer>();
		
		/*int num = get.nextInt();
		get.nextLine();
		for(int i=0;i<num;i++){
			System.out.println("Enter the observed variable and its value..");
			
			String key = get.nextLine();
			
			String val = get.nextLine();
			evidence.put(key, val);
		}*/
		System.out.println("Evidence : "+evidence.toString());
		Scanner read = new Scanner(fsdis);
		while(read.hasNext()){
			String nodes[] = read.nextLine().split("\\s+")[1].split(";");
			if(hmap.containsKey(nodes[0]) == false){
				JTNode temp = new JTNode();
				temp.set(nodes[0]);
				jtnodes.add(temp);
				hmap.put(nodes[0], hmap.size());
			}
			if(hmap.containsKey(nodes[1]) == false){
				JTNode temp = new JTNode();
				temp.set(nodes[1]);
				jtnodes.add(temp);
				hmap.put(nodes[1], hmap.size());
			}
			jtnodes.get(hmap.get(nodes[0])).addChild(jtnodes.get(hmap.get(nodes[1])));
			jtnodes.get(hmap.get(nodes[1])).addChild(jtnodes.get(hmap.get(nodes[0])));
		}
		
		for(int i =0;i<jtnodes.size();i++){
			visited.put(jtnodes.get(i).name, false);
			System.out.println(jtnodes.get(i).name);
			String file_name = changeName(jtnodes.get(i).name);
			Path src = new Path(basePath + "FYP/"+file_name+"/file.seq");
			Path dst = new Path(basePath + "FYP/"+file_name+"_copy/file.seq");
			if(fs.exists(new Path(basePath + "FYP/"+file_name+"_copy")))
				fs.delete((new Path(basePath + "FYP/"+file_name+"_copy")),true);
			SequenceFile.Reader reader = new SequenceFile.Reader(fs,src,conf);
			SequenceFile.Writer writer = SequenceFile.createWriter(fs,conf,dst,Text.class,CliqueNode.class);
			Text main_key = new Text("");
			CliqueNode main_val = new CliqueNode();
			reader.next(main_key, main_val);
			main_val.CPD = absorbEvidence(main_val.CPD , evidence);
			System.out.println(main_key.toString()+" :   "+main_val.CPD.toString());
			writer.append(main_key, main_val);
			reader.close();
			writer.close();
		}
		JTNode root = jtnodes.get(0);
		evidenceCollection(root);
		for(int i=0;i<jtnodes.size();i++)
			visited.put(jtnodes.get(i).name , false);
		if(fs.exists(new Path(basePath + "FYP/InferenceFinalOutput")))
			fs.delete(new Path(basePath + "FYP/InferenceFinalOutput"), true);
		FSDataOutputStream fsdos = fs.create(new Path(basePath + "FYP/InferenceFinalOutput/"+changeName(root.name)));
		FileStatus[] fss = fs.listStatus(new Path(basePath + "FYP/"+changeName(root.name)+"_copy"),new PathFilter() {
			  @Override
			  public boolean accept(Path path) {
			    return path.getName().endsWith(".seq") || path.getName().startsWith("part");
			  }
		    });
		SequenceFile.Reader reader = null;
		CliqueNode main_val = null;
		Text main_key = null;
		for (FileStatus status : fss) {
			Path path = status.getPath();
			System.out.println("READ PATH : "+path.toString());
	        reader = new SequenceFile.Reader(fs,path,conf);
			main_val = new CliqueNode();
			main_key = new Text();
			reader.next(main_key,main_val);
			if(!main_val.name.equals(""))
				break;
			
		}
		fsdos.writeBytes(main_val.CPD.toString());
		evidenceDistribution(root);
		fsdos.close();
		//Generating Output.
		Path outputPath = new Path(basePath + "FYP/InferenceFinalOutput");
		buildOutput(outputPath);
	}
	
	public HashMap<String,String> parse(String jsonLine) throws IOException{
		HashMap<String , String> evidence = new HashMap<String ,String>();
		JsonObject evidenceJSON = new JsonObject();
		JsonElement jelement = new JsonParser().parse(jsonLine);
		JsonObject  jobject = jelement.getAsJsonObject();
		JsonArray jarray = jobject.getAsJsonArray("assignments");
		jobject = jarray.get(0).getAsJsonObject();
		jarray = jobject.getAsJsonArray("response");
		for(int i=0;i<jarray.size();i++){
		    jobject = jarray.get(i).getAsJsonObject();
		    String key = jobject.get("skillId").toString();
		    String val = jobject.get("isCorrect").toString();
		    evidence.put(key.substring(1, key.length()-1) , val.substring(1, val.length()-1));
		    evidenceJSON.addProperty(key.substring(1, key.length()-1), val.substring(1, val.length()-1));
		}
		FileWriter fstream = new FileWriter("./evidence.json");
		BufferedWriter out = new BufferedWriter(fstream);
		out.write("var evidence = "+evidenceJSON.toString()+";");
		out.close();
		return evidence;
	}

	
	public static String getAssessmentByStudentID(String student) {
		try {

			URL url = new URL(
					"http://10.21.2.48:8080/orion-profile/getAssignmentRecord/"
							+ student);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();

			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");

			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "
						+ conn.getResponseCode());
			}

			BufferedReader br = new BufferedReader(new InputStreamReader(
					(conn.getInputStream())));

			String temp;
			StringBuffer sb = new StringBuffer();

			while ((temp = br.readLine()) != null) {
				sb.append(temp);
			}

			conn.disconnect();

			return sb.toString();

		} catch (MalformedURLException e) {

			e.printStackTrace();

		} catch (IOException e) {

			e.printStackTrace();

		}

		return null;
	}

	private void buildOutput(Path outputPath) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		//Run Map reduce job.
		Job job = new Job(conf);
		job.setJobName("Output Generator");
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setMapperClass(OutputBuilderMap.class);
		job.setReducerClass(OutputBuilderReduce.class);
		job.setJarByClass(OutputBuilder.class);
		FileInputFormat.addInputPath(job, outputPath);
		FileOutputFormat.setOutputPath(job, new Path(basePath + "FYP/OutputDump"));
		//job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		if (fs.exists(new Path(basePath + "FYP/OutputDump")))
			fs.delete(new Path(basePath + "FYP/OutputDump"), true);
		
		job.waitForCompletion(true);
		FileStatus[] fss = fs.listStatus(new Path(basePath + "FYP/OutputDump"),new PathFilter() {
		      @Override
		      public boolean accept(Path path) {
		        return path.getName().startsWith("part");
		      }
		    });
		FSDataInputStream reader; 
		Scanner read;
		FileWriter fstream = new FileWriter("./result.json");
		BufferedWriter out = new BufferedWriter(fstream);
		JsonObject result = new JsonObject();
		for (FileStatus status : fss) {
		    Path path = status.getPath();
		    reader = fs.open(path);
		    read = new Scanner(reader);
		    while(read.hasNext()){
		        String nodes[] = read.nextLine().split("\\s+");
		        result.addProperty(nodes[0], Double.parseDouble(nodes[1]));
		    }
		    
		}
		out.write("var result = "+result.toString()+";");
		out.close();
	}

	private HashMap<HashMap<String, String>, Double> absorbEvidence(
			HashMap<HashMap<String, String>, Double> CPD,
			HashMap<String, String> evidence) {
		Iterator<HashMap<String,String>> iter = CPD.keySet().iterator();
		while(iter.hasNext()){
			HashMap<String,String> current = iter.next();
			Iterator<String> iter2 = evidence.keySet().iterator();
			while(iter2.hasNext())
			{
				String key = iter2.next();
				if(current.containsKey(key) && !current.get(key).equals(evidence.get(key)))
					CPD.put(current, 0.0);
			}
		}
		return CPD;
	}

	private void evidenceDistribution(JTNode root) throws IOException, InterruptedException, ClassNotFoundException{
		visited.put(root.name, true);
		ArrayList<JTNode> children = new ArrayList<JTNode>();
		int len = root.children.size();
		if(len > 0){
			for(int i=0;i<len;i++)
				if(visited.get(root.children.get(i).name) == false)
					children.add(root.children.get(i));	
		}
		len = children.size();
		if(len > 0){
			//Run Map reduce job.
			Job job = new Job(conf);
			job.setJobName("Downward Pass for : " + root.name);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(CliqueNode.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(CliqueNode.class);
			job.setMapperClass(EvidenceDistributionMap.class);
			job.setReducerClass(EvidenceDistributionReduce.class);
			job.setJarByClass(EvidenceDistribution.class);
			FileInputFormat.addInputPath(job, new Path(basePath + "FYP/"+changeName(root.name)+"_copy"));
			FileOutputFormat.setOutputPath(job, new Path(basePath + "FYP/dummyoutput_copy"));
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			if (fs.exists(new Path(basePath + "FYP/dummyoutput_copy")))
				fs.delete(new Path(basePath + "FYP/dummyoutput_copy"), true);
			
			job.waitForCompletion(true);
			for(int i=0;i<len;i++)
				evidenceDistribution(children.get(i));
		}
	}

	private void evidenceCollection(JTNode root) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		visited.put(root.name, true);
		ArrayList<JTNode> children = new ArrayList<JTNode>();
		int len = root.children.size();
		if(len > 0){
			for(int i=0;i<len;i++)
				if(visited.get(root.children.get(i).name) == false)
					children.add(root.children.get(i));
		}
		len = children.size();
		if(len > 0){
				for(int i=0;i<len;i++)
					evidenceCollection(children.get(i));
			
			//open the children "sequence" files and make them in to one and invoke the map-reduce operation.
			SequenceFile.Reader reader = null;
			SequenceFile.Writer writer = SequenceFile.createWriter(fs,conf,new Path(basePath + "FYP/sample-input/file.seq"),CliqueNode.class,CliqueNode.class);
			FileStatus[] fss = fs.listStatus(new Path(basePath + "FYP/"+changeName(root.name)+"_copy"),new PathFilter() {
								  @Override
								  public boolean accept(Path path) {
								    return path.getName().endsWith(".seq") || path.getName().startsWith("part");
								  }
							    });
			CliqueNode main_val = new CliqueNode(),val = new CliqueNode();
			Text key;
			for (FileStatus status : fss) {
				Path path = status.getPath();
				System.out.println("PATH : "+path.toString());
		        reader = new SequenceFile.Reader(fs,path,conf);
				main_val = new CliqueNode();
				key = new Text();
				reader.next(key,main_val);
				
			}
			for(int i=0;i<len;i++){
				fss = fs.listStatus(new Path(basePath + "FYP/"+changeName(children.get(i).name)+"_copy"),new PathFilter() {
						  @Override
						  public boolean accept(Path path) {
							  return path.getName().endsWith(".seq") || path.getName().startsWith("part");
						  }
					  });
				for (FileStatus status : fss) {
					Path path = status.getPath();
					System.out.println("PATH Children: "+path.toString());
					reader = new SequenceFile.Reader(fs,path,conf);
					val = new CliqueNode();
					key = new Text();
					reader.next(key,val);
					if(!val.name.equals(""))
						writer.append(main_val, val);
					
				}
			}
			reader.close();
			writer.close();
			Job job = new Job(conf);
			job.setJobName("Upward Pass for : " + root.name);
			job.setMapOutputKeyClass(CliqueNode.class);
			job.setMapOutputValueClass(CliqueNode.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(CliqueNode.class);
			job.setMapperClass(EvidenceCollectionMap.class);
			job.setReducerClass(EvidenceCollectionReduce.class);
			job.setJarByClass(EvidenceCollection.class);
			FileInputFormat.addInputPath(job, new Path(basePath + "FYP/sample-input"));
			FileOutputFormat.setOutputPath(job, new Path(basePath + "FYP/"+changeName(root.name)+"_copy"));
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			if (fs.exists(new Path(basePath + "FYP/"+changeName(root.name)+"_copy")))
				fs.delete(new Path(basePath + "FYP/"+changeName(root.name)+"_copy"), true);
			
			job.waitForCompletion(true);
		}
		return;
	}
	
	private String changeName(String name) {
		String[] nodes = name.split(":");
		String file_name = "";
		for(int i=0;i<nodes.length-1;i++)
			file_name += nodes[i]+"_";
		file_name += nodes[nodes.length-1];
		return file_name;
	}

	public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException{
			new InferenceRunner();
		}
}
