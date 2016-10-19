package org.myorg;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;



public class OutputBuilder{
	
	private static HashMap<HashMap<String, String>, Double> stringToHashMap(String hmap) {
		System.out.println(hmap);
		HashMap<HashMap<String,String>,Double> CPD = new HashMap<HashMap<String,String>,Double>();
		hmap = hmap.replaceAll("\\s+", "");
		hmap = hmap.substring(1, hmap.length()-1);
		
		String map[] = hmap.split("\\{");
		for(int i=1;i<map.length;i++){
			String temp[];
			if(i == map.length-1)
				temp = map[i].split("\\}");
			else
				temp = map[i].substring(0,map[i].length()-1).split("\\}");
			Double d = Double.parseDouble(temp[1].substring(1));
			
			temp = temp[0].split(",");
			HashMap<String,String> InnerMap = new HashMap<String,String>();
			for(int j=0;j<temp.length;j++){
				String nodeValue[] = temp[j].split("=");
				InnerMap.put(nodeValue[0], nodeValue[1]);
			}
			CPD.put(InnerMap, d);
			
		}
		return CPD;
	}
	
	public static class OutputBuilderMap extends Mapper<LongWritable, Text, Text,DoubleWritable> {
		
		@Override
		public void map(LongWritable key,Text value, Context context)
				throws IOException, InterruptedException {
			System.out.println(key.toString() + "---" + value.toString());
			HashMap<HashMap<String,String>,Double> CPD = stringToHashMap(value.toString());
			Iterator<HashMap<String,String>> iter = CPD.keySet().iterator();
			double total = 0.0;
			HashMap<String , Double> marginals = new HashMap<String,Double>();
			while(iter.hasNext()){
				HashMap<String,String> current = iter.next();
				total += CPD.get(current);
				Iterator<String> iter2 = current.keySet().iterator();
				System.out.println(current.toString());
				while(iter2.hasNext()){
					String node = iter2.next();
					System.out.println(node);
					if(current.get(node).equals("TRUE")){
						System.out.println(node+"----"+CPD.get(current));
						//context.write(new Text(node) , new DoubleWritable(CPD.get(current)));
						if(marginals.containsKey(node))
							marginals.put(node, marginals.get(node)+CPD.get(current));
						else
							marginals.put(node , CPD.get(current));
					}
				}
			}
			Iterator<String> iter2 = marginals.keySet().iterator();
			while(iter2.hasNext()){
				String t = iter2.next();
				context.write(new Text(t) , new DoubleWritable(marginals.get(t) / total));
			}
		}
			
			
	}

		
	
	public static class OutputBuilderReduce extends Reducer<Text,DoubleWritable, Text, DoubleWritable> {
		
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			
			Double result = 0.0;
			for(DoubleWritable d : values){
				result = d.get();
			}
			System.out.println(key.toString()+"---"+result);
			context.write(key, new DoubleWritable(result));
		}

		
	}
}
