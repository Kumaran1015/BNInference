//package org.myorg;
 

 
import java.io.IOException;
import java.util.*;
 
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.*;
 
public class Moralize {
 

  static ArrayList<String> adjacency = new ArrayList<String>();

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    //private final static IntWritable one = new IntWritable(1);
    
 
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      String line = value.toString();
      String[] nodes = line.split("\\t");
	for(int i=1;i<nodes.length;i++)
	{
		System.out.println(nodes[i]+"    "+nodes[0]);
        	output.collect(new Text(nodes[i]), new Text(nodes[0]));
		
	}
      
    }
  }


  public static class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    //private final static IntWritable one = new IntWritable(1);
    
 
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      String line = value.toString();
      String[] nodes = line.split("\\t");
	for(int i=1;i<nodes.length;i++)
	{
        	output.collect(new Text(nodes[0]), new Text(nodes[i]));
		System.out.println(nodes[0]+"    "+nodes[i]);
	}
      
    }
  }
 
  public static class JT_Map1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	    //private final static IntWritable one = new IntWritable(1);
	    
	 
	    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		System.out.println("hi in map...");
	    	String line = value.toString();
	    	String cliq = "";
	        String[] nodes = line.split("\\s+");
		System.out.println(line);
	        for(int i= 0;i<nodes.length-1;i++)
			{
		        	cliq += nodes[i]+':'; 
				
			}
	        cliq += nodes[nodes.length-1];
		System.out.println(line);
	        Text word = new Text(cliq);
			for(int i= 0;i<nodes.length;i++)
			{
				System.out.println(nodes[i] +"----" + cliq);
		        	output.collect(new Text(nodes[i]), word);
				
			}
	      
	    }
	    
	  }
  
  
  public static class JT_Map2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	    //private final static IntWritable one = new IntWritable(1);
	    
	 
	    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		System.out.println("hi in map...");
	    	String line = value.toString();
	        String[] nodes = line.split("\\s+");
	        System.out.println(line);
		    output.collect(new Text(nodes[0]), new Text(nodes[1]));
	
	      
	    }
	    
	  }
  
  public static class JT_Map3 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	    //private final static IntWritable one = new IntWritable(1);
	    
	 
	    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		System.out.println("hi in map...");
	    	String line = value.toString();
	        String[] clusters = line.split("\\s+");
	        System.out.println(line);
	        String[] nodes = clusters[1].split(":");
	        Integer l = nodes.length;
		    output.collect(new Text(l.toString()), new Text(clusters[0] + "\t"+ clusters[1]));
	
	      
	    }
	    
	  }


//Reverse sorter.
static class ReverseComparator extends WritableComparator {
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public ReverseComparator() {
            super(Text.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            
                return (-1)* TEXT_COMPARATOR
                        .compare(b1, s1, l1, b2, s2, l2);
           
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof Text && b instanceof Text) {
                return (-1)*(((Text) a)
                        .compareTo((Text) b));
            }
            return super.compare(a, b);
        }
    }

  
  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {


 public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

	
	String temp = "";
	String t;
	ArrayList<String> arr = new ArrayList<String>();
	System.out.println("KEY : "+key.toString());
      while (values.hasNext()) {
	t = values.next().toString();
	temp += t+"\t";
	arr.add(t);
      }
	output.collect(key,new Text(temp));
	int len = arr.size();
	for(int i=0;i<len;i++)
	{
		for(int j=i+1;j<len;j++)
		{
			output.collect(new Text(arr.get(i)) , new Text(arr.get(j)));
			output.collect(new Text(arr.get(j)) , new Text(arr.get(i)));
		}
	}
      
    }


  }



public static class Reduce2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {


 public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

	
	String temp = "";
	String t;
	
	System.out.println("KEY : "+key.toString());
      while (values.hasNext()) {
	t = values.next().toString();
	temp += t+"\t";
      }
	output.collect(key,new Text(temp));
	
      
    }
    


  }


public static class JT_Reduce1 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {


	 public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

		
		System.out.println("hi in reduce...");
		String t;
		ArrayList<String> arr = new ArrayList<String>();
		
	      while (values.hasNext()) {
		t = values.next().toString();
		System.out.println(t);
		arr.add(t);
	      }
		
		int len = arr.size();
		for(int i=0;i<len;i++)
		{
			for(int j=i+1;j<len;j++)
			{
				String temp = "";
				if(arr.get(i).compareTo(arr.get(j)) < 0)
					temp = arr.get(i)+","+arr.get(j);
				else
					temp = arr.get(j)+","+arr.get(i);
				output.collect(new Text(temp) ,key);
				System.out.println(temp+"-----"+key.toString());
				
			}
		}
	      
	    }


	  }




public static class JT_Reduce2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {


	 public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

		
		System.out.println("hi in reduce...");
		String t;
		
		String sepset = "";
		
	      while (values.hasNext()) {
	    	 
	    	  t = values.next().toString();
	    	  System.out.println(t);
	    	  sepset += t + ":";
	      }
		
		Text emit_value = new Text(sepset);
				output.collect(key ,emit_value);
				
				
		
	      
	    }


	  }




public static class JT_Reduce3 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {


	 public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

		
		System.out.println("hi in reduce...");
		String t;
	      while (values.hasNext()) {
	    	 
	    	  t = values.next().toString();
	    	  System.out.println(t);
	    	  output.collect(key ,new Text(t));
	      }
	      
	    }

	  }



//Sorting map-reduce.

	public static class Sort_Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
	    //private final static IntWritable one = new IntWritable(1);
	    
	
	    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
	      String line = value.toString();
		String nodes[] = line.split("\\s+");
		/*System.out.println();
		for(int j=0;j<nodes.length ; j++)
		{
			System.out.print(j+1 +"  "+nodes[j]+" ");
		}*/
		Integer i = nodes.length - 1;
	      //Text length = new Text(i.toString());
		IntWritable length = new IntWritable(i);
		System.out.println("Hi "+value.toString()+"key : "+length.get());
	        output.collect(length , value);
	    }
	}

 public static class Sort_Reduce extends MapReduceBase implements Reducer<IntWritable, Text, Text, Text> {


 public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

	
	
	//System.out.println("KEY : "+key.toString());
      while (values.hasNext()) {
	String str = values.next().toString();
	System.out.println("Hi "+str +"Key :"+ key.toString());
	output.collect(new Text(str),new Text(""));
		
      }
	
	
      
    }


  }


 
 public static class Merge_Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	    //private final static IntWritable one = new IntWritable(1);
	    
	
	    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	    String line = value.toString();
	    String nodes[] = line.split("\\s+");
		
	    for(int i=1;i<nodes.length;i++)
	    	output.collect(new Text(nodes[0]), new Text(nodes[i]));
	    }
	}
 
 
 
 public static class Merge_Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	 public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		 
		//System.out.println("KEY : "+key.toString());
		 HashSet<String> bucket = new HashSet<String>();
	      while (values.hasNext()) {
	    	  bucket.add(values.next().toString());
	      }
	      String t = "";
	      Iterator<String> it = bucket.iterator();
	      while(it.hasNext())
	    	  t += it.next() + "\t";
	      output.collect(key , new Text(t));
	 }
}
 
  public static void main(String[] args) throws Exception {


    System.out.println("Moralizing....");
    JobConf conf = new JobConf(Moralize.class);
    conf.setJobName("moralize");
 
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
 
    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);
 
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
	
 
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    
    JobClient.runJob(conf);
    FileSystem fs = FileSystem.get(conf);
    fs.copyFromLocalFile(new Path("/home/training/FYP/moralize/file01"),new Path(args[1]));
    fs.copyFromLocalFile(new Path("/home/training/FYP/moralize/file02"),new Path(args[1]));
	
    JobConf conf2 = new JobConf(Moralize.class);
    conf2.setJobName("moralize2");
 
    conf2.setOutputKeyClass(Text.class);
    conf2.setOutputValueClass(Text.class);
 
    conf2.setMapperClass(Map2.class);
    conf2.setCombinerClass(Reduce2.class);
    conf2.setReducerClass(Reduce2.class);
 
    conf2.setInputFormat(TextInputFormat.class);
    conf2.setOutputFormat(TextOutputFormat.class);
	
 
    FileInputFormat.setInputPaths(conf2, new Path(args[1]));
    FileOutputFormat.setOutputPath(conf2, new Path(args[2]));

    
    JobClient.runJob(conf2);


	//sorting the moralized graph for minimum adjacencies.


    /*

    JobConf conf3 = new JobConf(Moralize.class);
    conf3.setJobName("Sorting");
 
    conf3.setMapOutputKeyClass(IntWritable.class);
    conf3.setMapOutputValueClass(Text.class);
    conf3.setOutputKeyClass(Text.class);
    conf3.setOutputValueClass(Text.class);
 
    conf3.setMapperClass(Sort_Map.class);
    //conf3.setCombinerClass(Sort_Reduce.class);
    conf3.setReducerClass(Sort_Reduce.class);
 
    conf3.setInputFormat(TextInputFormat.class);
    conf3.setOutputFormat(TextOutputFormat.class);
	
 
    FileInputFormat.setInputPaths(conf3, new Path(args[2]));
    FileOutputFormat.setOutputPath(conf3, new Path("/user/training/moralize/sorting_output"));

    
    JobClient.runJob(conf3);
*/
     
     //Triangulation
    System.out.println("Triangulation...");
    FSDataInputStream fsdis = fs.open(new Path("/user/training/moralize/output/part-00000"));
    HashMap adj = new HashMap();
    
    Scanner read = new Scanner(fsdis);
    String[] cliq = null;
    while(read.hasNext())
    {
    	String t = read.nextLine();
    	cliq = t.split("\\s+");
    	String ind = cliq[0];
    	HashSet<String> temp = new HashSet<String>();
    	for(int i=1;i<cliq.length;i++)
    		temp.add(cliq[i]);
    	adj.put(ind , temp);
    }
    fsdis.close();
    fsdis = fs.open(new Path("/user/training/moralize/output/part-00000"));
    read = new Scanner(fsdis);
    String targ = null;
    int min = 1000;
    while(read.hasNext())
    {
    	String t = read.nextLine();
    	cliq = t.split("\\s+");
    	int count = 0;
	
    	for(int i=1;i<cliq.length;i++)
    	{
    		for(int j=i+1;j<cliq.length;j++)
    		{
    			HashSet<String> hs = (HashSet<String>) adj.get(cliq[i]);
    			if(hs.contains(cliq[j]) == false)
    			{
    				count++;
    			}
    		}
    		
    	}
    	if(count <= min)
		{
			targ = t;
			min = count;
		}
    	
		
    }
    
    
    
    
    ArrayList<HashSet<String>> Cliques = new ArrayList<HashSet<String>>();
    Boolean done = false;
    HashSet<String> visited = new HashSet<String>();
    FSDataOutputStream fsdos;
    while(!done)
    {
    cliq = targ.split("\\s+");
    visited.add(cliq[0]);
    HashSet hs = new HashSet();
    for(int i=0;i<cliq.length;i++)
    	hs.add(cliq[i]);
    boolean contain = false;
    for(int i=0;i<Cliques.size();i++)
    	if(Cliques.get(i).containsAll(hs))
    		contain = true;
    if(contain == false)
    	Cliques.add(hs);
    
    fsdis = fs.open(new Path("/user/training/moralize/output/part-00000"));
    fsdos = fs.create(new Path("/user/training/triangulation/input/file01"));
    read = new Scanner(fsdis);
    while(read.hasNext())
    {
    	fsdos.writeBytes(read.nextLine()+"\n");
    }
   
	
	
	fsdis.close();
	fsdos.close();
	
	fsdos = fs.create(new Path("/user/training/triangulation/input/file02"));
	for(int i = 1 ; i < cliq.length ; i++)
	{
		for(int j = i+1 ; j < cliq.length ; j++)
		{
			String t = cliq[i].toString() + "\t" + cliq[j].toString();
			fsdos.writeBytes(t+"\n");
			t = cliq[j].toString() + "\t" + cliq[i].toString();
			fsdos.writeBytes(t+"\n");
		}
	}
   fsdos.close();
    //join adjacencies ---- job.
	
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
	
	fs.delete(new Path("/user/training/joinadj/output"),true);
   FileInputFormat.setInputPaths(conf4, new Path("/user/training/triangulation/input"));
   FileOutputFormat.setOutputPath(conf4, new Path("/user/training/joinadj/output"));

   
   JobClient.runJob(conf4);
	
	
	fsdis = fs.open(new Path("/user/training/moralize/output/part-00000"));
    read = new Scanner(fsdis);
    min = 1000;
    done = true;
    while(read.hasNext())
    {
    	String t = read.nextLine();
    	cliq = t.split("\\s+");
    	int count = 0;
	String curCliq = cliq[0];
    	if(visited.contains(cliq[0]))
    		continue;
    	done = false;
    	for(int i=1;i<cliq.length;i++)
    	{
    		for(int j=i+1;j<cliq.length;j++)
    		{
    			HashSet<String> has = (HashSet<String>) adj.get(cliq[i]);
    			if(visited.contains(cliq[i]) == false && visited.contains(cliq[j]) == false && has.contains(cliq[j]) == false)
    			{
    				count++;
    			}
    		}
    		
    	}
    	if(count <= min)
		{
			targ = cliq[0];
			for(int i=1;i<cliq.length;i++)
				if(visited.contains(cliq[i]) == false)
					targ += "\t"+cliq[i];
			min = count;
		}
    	
		
    }


	
	
    }
	//end of triangulation.
    fsdos = fs.create(new Path("/user/training/maximal_clique/output/part-00000"));
	for(int i=0;i<Cliques.size();i++)
	{
	Iterator<String> it = Cliques.get(i).iterator();
    	while(it.hasNext())
    		fsdos.writeBytes(it.next()+"\t");
    	//System.out.print(it.next());
	//System.out.println("\n");
    	fsdos.writeBytes("\n");
	}
	fsdos.close();
	//Generated maximal cliques.
	
	
	
	//start of junction tree construction.
	System.out.println("Junction Tree Construction....");

	JobConf conf6 = new JobConf(Moralize.class);
    conf6.setJobName("Junction tree construction - part1");
 
    conf6.setOutputKeyClass(Text.class);
    conf6.setOutputValueClass(Text.class);
 
    conf6.setMapperClass(JT_Map1.class);
    //conf6.setCombinerClass(JT_Reduce1.class);
    conf6.setReducerClass(JT_Reduce1.class);
 
    conf6.setInputFormat(TextInputFormat.class);
    conf6.setOutputFormat(TextOutputFormat.class);
	
 
    FileInputFormat.setInputPaths(conf6, new Path("/user/training/maximal_clique/output"));
    FileOutputFormat.setOutputPath(conf6, new Path("/user/training/junction_tree/output"));

    
    JobClient.runJob(conf6);
    
    //Forming the separation sets.
    
    JobConf conf7 = new JobConf(Moralize.class);
    conf7.setJobName("Junction tree construction - part2");
 
    conf7.setOutputKeyClass(Text.class);
    conf7.setOutputValueClass(Text.class);
 
    conf7.setMapperClass(JT_Map2.class);
    conf7.setReducerClass(JT_Reduce2.class);
 
    conf7.setInputFormat(TextInputFormat.class);
    conf7.setOutputFormat(TextOutputFormat.class);
	
 
    FileInputFormat.setInputPaths(conf7, new Path("/user/training/junction_tree/output"));
    FileOutputFormat.setOutputPath(conf7, new Path("/user/training/junction_tree/output2"));

    
    JobClient.runJob(conf7);
    
    //Sorting the separation sets.
    JobConf conf8 = new JobConf(Moralize.class);
    conf8.setJobName("Junction tree construction - part3");
 
    conf8.setOutputKeyClass(Text.class);
    conf8.setOutputValueClass(Text.class);
 
    conf8.setMapperClass(JT_Map3.class);
    conf8.setReducerClass(JT_Reduce3.class);
    conf8.setOutputKeyComparatorClass(ReverseComparator.class);
    conf8.setInputFormat(TextInputFormat.class);
    conf8.setOutputFormat(TextOutputFormat.class);
	
 
    FileInputFormat.setInputPaths(conf8, new Path("/user/training/junction_tree/output2"));
    FileOutputFormat.setOutputPath(conf8, new Path("/user/training/junction_tree/sorted_output"));

    
    JobClient.runJob(conf8);

  }
  
  

}