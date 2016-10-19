package org.myorg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;



public class EvidenceCollection {
	
	static String basePath = "";
	private static void recurse(HashMap<HashMap<String, String>, Double> hmap,
			HashMap<String, String> map, int index, ArrayList<Node> nodes,double initialValue) {
		if (index == nodes.size()) {

			@SuppressWarnings("unchecked")
			HashMap<String, String> dummy = (HashMap<String, String>)map.clone();
			hmap.put(dummy, initialValue);

		} else {
			Node cur = nodes.get(index);
			for (String value : cur.values) {
				map.put(cur.name, value);

				recurse(hmap, map, index + 1, nodes,initialValue);
			}
		}
	}
	
	public static class EvidenceCollectionMap extends Mapper<CliqueNode, CliqueNode, CliqueNode,CliqueNode> {
		
		
		@Override
		public void map(CliqueNode key,CliqueNode value, Context context)
				throws IOException, InterruptedException {
			String[][] dependencies = value.dependencies;
			ArrayList<Node> nodes = new ArrayList<Node>();
			String[] currentClique =key.name.split(":");
			for(int i=0;i<dependencies.length;i++){
				for(int j=0;j<currentClique.length;j++){
					if(currentClique[j].equals(dependencies[i][0])){
						Node temp = new Node();
						temp.name = currentClique[j];
						temp.values = new String[dependencies[i].length-1];
						for(int k=1;k<dependencies[i].length;k++)
							temp.values[k-1] = dependencies[i][k];
						nodes.add(temp);
					}
				}
			}
			if(nodes.size() == 0)
				value.CPD = new HashMap<HashMap<String,String>,Double>();
			else
			{
				HashMap<HashMap<String,String>,Double> hmap = new HashMap<HashMap<String,String>,Double>();
				HashMap<String , String> map = new HashMap<String , String>();
				recurse(hmap, map, 0, nodes ,0.0);
				HashMap<HashMap<String,String>,Double> source_hmap = value.CPD;
				Iterator<HashMap<String,String>> iter =  source_hmap.keySet().iterator();
				while(iter.hasNext()){
					HashMap<String,String> source_map = iter.next();
					HashMap<String,String> temp = new HashMap<String,String>();
					for(int i=0;i<nodes.size();i++){
						temp.put(nodes.get(i).name, source_map.get(nodes.get(i).name));
					}
					
					hmap.put(temp, source_hmap.get(source_map)+hmap.get(temp));
					
				}
				value.CPD = hmap;
				System.out.println("Hmap result : "+hmap.toString());
			}
			
			System.out.println("Emit : ["+key.name+","+value.name+"]");
			context.write(key, value);
			
		}
	}
	
	
	public static class EvidenceCollectionReduce extends Reducer<CliqueNode,CliqueNode, Text, CliqueNode> {
		
		
		public void reduce(CliqueNode key, Iterable<CliqueNode> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<CliqueNode> multipliers = new ArrayList<CliqueNode>();
			multipliers.add(key);
			key.incomingMessage = new HashMap<String,HashMap<HashMap<String,String>,Double>>();
			CliqueNode clone = null;
			ArrayList<String> string_keys = new ArrayList<String>();
			ArrayList<HashMap<HashMap<String,String>,Double>> value_maps = new ArrayList<HashMap<HashMap<String,String>,Double>>();
			for(CliqueNode log : values){
				System.out.println("Child : " +log.name);
				clone = new CliqueNode(log);
				multipliers.add(new CliqueNode(log));
				string_keys.add(clone.name);
				value_maps.add(clone.CPD);
			}
			for(int i=0;i<string_keys.size();i++){
				key.incomingMessage.put(string_keys.get(i), value_maps.get(i));
				System.out.println("Incoming Msg : "+key.incomingMessage.toString());
			}
			HashMap<HashMap<String,String>,Double> hmap = new HashMap<HashMap<String,String>,Double>();
			
			ArrayList<Node> nodes = new ArrayList<Node>();
			HashMap<String,Boolean> visited = new HashMap<String,Boolean>();
			String nodeArray[] = key.name.split(":");
			for(String t : nodeArray)
					visited.put(t,false);
			
		
			for(CliqueNode log: multipliers){
				System.out.println("Multiplier : " +log.name);
				String[][] dependencies = log.dependencies;
				for(int i=0;i<dependencies.length;i++){
					if(!visited.containsKey(dependencies[i][0]))continue;
					if(visited.get(dependencies[i][0]) == true) continue;
					Node temp = new Node();
					temp.name = dependencies[i][0];
					temp.values = new String[dependencies[i].length-1];
					for(int k=1;k<dependencies[i].length;k++)
						temp.values[k-1] = dependencies[i][k];
					nodes.add(temp);
					visited.put(temp.name,true);
				}
			}
			HashMap<String , String> map = new HashMap<String , String>();
			recurse(hmap, map, 0, nodes,1.0);
			
	
			
			for(CliqueNode log : multipliers){
				
				HashMap<HashMap<String,String>,Double> hmap_inner = log.CPD;
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
							Double ref = hmap.get(temp2);
							hmap.put(temp2,
									ref * hmap_inner.get(temp));
						}
					}
				}
			}
			key.CPD = hmap;
			System.out.println(key.name+" : "+hmap.toString());
			context.write(new Text(key.name), key);
			
		}
	}
}
