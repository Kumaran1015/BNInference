package org.myorg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

class CliqueNode implements WritableComparable<CliqueNode> 
{
	String name;
	String[][] dependencies;
	HashMap<HashMap<String,String> , Double> CPD;
	HashMap<String , HashMap<HashMap<String,String> , Double>> incomingMessage;
	CliqueNode(){	
		this.name = "";
		this.dependencies = new String[0][0];
		this.CPD = new HashMap<HashMap<String,String> , Double>();
		this.incomingMessage = new HashMap<String , HashMap<HashMap<String,String> , Double>>();
	}
	CliqueNode(CliqueNode o){
		this.name = o.name;
		this.CPD = o.CPD;
		this.incomingMessage = o.incomingMessage;
		this.dependencies = new String[o.dependencies.length][];
		for(int i=0;i<this.dependencies.length;i++){
			this.dependencies[i] = new String[o.dependencies[i].length];
			for(int j=0;j<this.dependencies[i].length;j++)
				this.dependencies[i][j] = o.dependencies[i][j];
		}
		
	}
	CliqueNode(String name){
		this.name = name;
		this.dependencies = new String[0][0];
		this.CPD = new HashMap<HashMap<String,String> , Double>();
		this.incomingMessage = new HashMap<String , HashMap<HashMap<String,String> , Double>>();
	}
	void set(String name ){
		this.name = name;
	}
	
	void setdependencies(Node[] dependencies){
		this.dependencies = new String[dependencies.length][];
		for(int i = 0;i<dependencies.length ; i++){
			this.dependencies[i] = new String[dependencies[i].values.length+1];
			this.dependencies[i][0] = dependencies[i].name;
			for(int j = 1;j<dependencies[i].values.length+1;j++){
				this.dependencies[i][j] = dependencies[i].values[j-1];
			}
		}
	}
	
	void setCPD(HashMap<HashMap<String,String> , Double> CPD){
		this.CPD = CPD;
	}
	String getName(){
		return name;
	}
	
	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		//out.writeBytes(this.name);
		Text put = new Text(this.name);
		put.write(out);
		
		
		//writing the dependency data.
		out.writeInt(this.dependencies.length);
		for(int i = 0 ; i < this.dependencies.length ; i++){
			out.writeInt(this.dependencies[i].length);
			for(int j = 0 ; j < this.dependencies[i].length ; j++){
				put = new Text(this.dependencies[i][j]);
				put.write(out);
			}
		}
		
		//writing the CPD.
	
		put = new Text(this.CPD.toString());
		put.write(out);
		
		//writing the incoming Messages.
		if(this.incomingMessage == null)
			out.writeInt(0);
		else{
			out.writeInt(this.incomingMessage.size());
			Iterator<String> iterator = this.incomingMessage.keySet().iterator();
			while(iterator.hasNext()){
				String temp = iterator.next();
				put = new Text(temp);
				put.write(out);
				put = new Text(this.incomingMessage.get(temp).toString());
				put.write(out);
			}
		}
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		Text get = new Text("");
		get.readFields(in);
		this.name = get.toString();
	
		
		
		//reading the dependencies data.
		int size = in.readInt();//out.writeInt(this.dependencies.length);
		this.dependencies = new String[size][];
		for(int i = 0 ; i < size ; i++){
			int s = in.readInt();
			this.dependencies[i] = new String[s];
			for(int j = 0 ; j < s ; j++){
				get.readFields(in);
				this.dependencies[i][j] = get.toString();
			}
		}
		
		//reading the CPD.
		get.readFields(in);
		String hmap = get.toString();
		this.CPD = stringToHashMap(hmap);
		this.incomingMessage = new HashMap<String , HashMap<HashMap<String,String> , Double>>();
		//reading the incoming Messages.
		size = in.readInt();
		for(int i=0;i<size;i++){
			get.readFields(in);
			String key = get.toString();
			get.readFields(in);
			String value = get.toString();
			this.incomingMessage.put(key, stringToHashMap(value));
		}
		
	}
	private HashMap<HashMap<String, String>, Double> stringToHashMap(String hmap) {
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
	@Override
	public int compareTo(CliqueNode o) {
		// TODO Auto-generated method stub
		//return 0;
		if(this.name.equals(o.name))
			return 0;
		else
			return 1;
	}
	
}
