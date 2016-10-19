package org.myorg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;



public class EvidenceDistribution{
	static String basePath = "";
	public static class EvidenceDistributionMap extends Mapper<Text, CliqueNode, Text,CliqueNode> {
		
		
		@Override
		public void map(Text key,CliqueNode value, Context context)
				throws IOException, InterruptedException {
			HashMap<HashMap<String,String> , Double> numerator = new HashMap<HashMap<String,String> , Double>(value.CPD);
			Iterator<String> iter = value.incomingMessage.keySet().iterator();
			System.out.println("Root : "+key.toString());
			System.out.println("Numerator : "+value.CPD.toString());
			System.out.println("Incoming Msg : "+value.incomingMessage.toString());
			while(iter.hasNext()){
				String str = iter.next();
				HashMap<HashMap<String,String> , Double> denominator = value.incomingMessage.get(str);
				HashMap<HashMap<String,String> , Double> quotient = divide(numerator,denominator);
				numerator = new HashMap<HashMap<String,String> , Double>(value.CPD);
				System.out.println("Node : "+str);
				System.out.println("Quotient : " + quotient.toString());
				HashMap<HashMap<String,String> , Double> result = new HashMap<HashMap<String,String> , Double>();
				Iterator<HashMap<String,String>> iter2 =  quotient.keySet().iterator();
				String[] nodes =str.split(":");
				boolean contains = false;
				while(iter2.hasNext()){
					HashMap<String,String> source_map = iter2.next();
					HashMap<String,String> temp = new HashMap<String,String>();
					for(int i=0;i<nodes.length;i++){
						if(source_map.containsKey(nodes[i])){
							contains = true;
							temp.put(nodes[i], source_map.get(nodes[i]));
						}
					}
					if(result.containsKey(temp))
						result.put(temp, quotient.get(source_map)+result.get(temp));
					else
						result.put(temp, quotient.get(source_map));
				}
				
				CliqueNode emit = new CliqueNode();
				if(contains  == true)
					emit.CPD = result;
				else
					emit.CPD = new HashMap<HashMap<String,String> , Double>();
				emit.name = str;
				emit.dependencies = new String[0][0];
				emit.incomingMessage = null;
				
				System.out.println("Result : " + result.toString());
				context.write(new Text(str), emit);
			}
			
			
		}

		private HashMap<HashMap<String, String>, Double> divide(
				HashMap<HashMap<String, String>, Double> numerator,
				HashMap<HashMap<String, String>, Double> denominator) {
			// TODO Auto-generated method stub
			//HashMap<HashMap<String,String>,Double> hmap_inner = denominator;
			Iterator<HashMap<String, String>> iter = denominator.keySet().iterator();
			while (iter.hasNext()) {
				HashMap<String, String> temp = iter.next();
				Iterator<HashMap<String, String>> iter2 = numerator.keySet()
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
						Double ref = numerator.get(temp2);
						Double ans = ref / denominator.get(temp);
						System.out.println("division of "+ref+" by "+denominator.get(temp)+"gives "+ans);
						if(Double.isNaN(ans))
						{
							numerator.put(temp2,0.0);
							System.out.println("identified");
						}
						else
							numerator.put(temp2,ans);
						
					}
				}
			}
			return numerator;
		}
	}
	
	
	public static class EvidenceDistributionReduce extends Reducer<Text,CliqueNode, Text, CliqueNode> {
		
		
		public void reduce(Text key, Iterable<CliqueNode> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<CliqueNode> multipliers = new ArrayList<CliqueNode>();
			String file_name = changeName(key.toString())+"_copy";
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(new Path(basePath + "FYP/"+file_name),new PathFilter() {
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
				
			}
			multipliers.add(main_val);
			for(CliqueNode log : values)
				multipliers.add(log);
			
			System.out.println("Node : "+key.toString());
			System.out.println("Multiplier 1 : "+ multipliers.get(0).CPD.toString());
			System.out.println("Multiplier 2 : "+ multipliers.get(1).CPD.toString());
			HashMap<HashMap<String,String> , Double> product = multiply(multipliers.get(0).CPD,multipliers.get(1).CPD);
			main_val.CPD = product;
			System.out.println("Product : "+product.toString());
			FSDataOutputStream fsdos = fs.create(new Path(basePath + "FYP/InferenceFinalOutput/"+changeName(key.toString())));
			System.out.println("File handle : "+fsdos);
			fsdos.writeBytes(main_val.CPD.toString());
		}

		
		private HashMap<HashMap<String, String>, Double> multiply(
				HashMap<HashMap<String, String>, Double> cpd1,
				HashMap<HashMap<String, String>, Double> cpd2) {
			// TODO Auto-generated method stub
			HashMap<HashMap<String, String>, Double> result = new HashMap<HashMap<String, String>, Double>();
			Iterator<HashMap<String, String>> iter = cpd1.keySet().iterator();
			while (iter.hasNext()) {
				HashMap<String, String> temp = iter.next();
				Iterator<HashMap<String, String>> iter2 = cpd2.keySet()
						.iterator();
				while (iter2.hasNext()) {
					HashMap<String, String> temp2 = iter2.next();
					Iterator<String> iter3 = temp.keySet().iterator();
					boolean match = true;
					while (iter3.hasNext()) {
						String search = iter3.next();
						if(temp2.containsKey(search))
							if (!(temp2.get(search).equals(temp.get(search))))
								match = false;
					}
					if (match == true) {
						HashMap<String,String> map = new HashMap<String,String>();
						map.putAll(temp);
						map.putAll(temp2);
						result.put(map, cpd1.get(temp) * cpd2.get(temp2));
					}
				}
			}
			return result;
		}

		private String changeName(String name) {
			String[] nodes = name.split(":");
			String file_name = "";
			for(int i=0;i<nodes.length-1;i++)
				file_name += nodes[i]+"_";
			file_name += nodes[nodes.length-1];
			return file_name;
		}
	}
}
