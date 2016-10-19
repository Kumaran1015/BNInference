package org.myorg;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;

import java.io.IOException;



import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

class Node implements WritableComparable<Node> 
{
	String name;
	String[] values;
	String[][] parents;
	double[][] CPD;
	Node(){	}
	Node(String name, String[] values){
		this.name = name;
		this.values = new String[values.length];
		for(int i = 0;i < values.length; i++)
			this.values[i] = values[i];
	}
	void set(String name , String[] values){
		this.name = name;
		this.values = new String[values.length];
		for(int i = 0;i < values.length; i++)
			this.values[i] = values[i];
	}
	
	void setParents(Node[] parents){
		this.parents = new String[parents.length][];
		for(int i = 0;i<parents.length ; i++){
			this.parents[i] = new String[parents[i].values.length+1];
			this.parents[i][0] = parents[i].name;
			for(int j = 1;j<parents[i].values.length+1;j++){
				this.parents[i][j] = parents[i].values[j-1];
			}
		}
	}
	
	void setCPD(double[][] CPD){
		
		this.CPD = new double[CPD.length][CPD[0].length];
		for(int i = 0; i < CPD.length ; i++){
			for(int j = 0; j < CPD[i].length ; j++)
				this.CPD[i][j] = CPD[i][j];
		}
		
	}
	String getName(){
		return name;
	}
	
	String[] getValues(){
		return values;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		//out.writeBytes(this.name);
		Text put = new Text(this.name);
		put.write(out);
		out.writeInt(this.values.length);
		for(String val : this.values){
			//out.writeBytes(val);
			put = new Text(val);
			put.write(out);
		}
		
		//writing the parent data.
		out.writeInt(this.parents.length);
		for(int i = 0 ; i < this.parents.length ; i++){
			out.writeInt(this.parents[i].length);
			for(int j = 0 ; j < this.parents[i].length ; j++){
				put = new Text(this.parents[i][j]);
				put.write(out);
			}
		}
		
		//writing the CPD.
		
		out.writeInt(this.CPD.length);
		for(int i = 0 ; i < this.CPD.length ; i++){
			out.writeInt(this.CPD[i].length);
			for(int j = 0 ; j < this.CPD[i].length ; j++){
				out.writeDouble(this.CPD[i][j]);
			}
		}
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		Text get = new Text("");
		get.readFields(in);
		this.name = get.toString();
	
		System.err.print("name " + this.name);
		int size = in.readInt();
		System.err.println("size " + size);
		this.values = new String[size];
		for(int i = 0 ; i < size ; i++){
			get.readFields(in);
			this.values[i] = get.toString();
		}
		
		//writing the parent data.
		size = in.readInt();//out.writeInt(this.parents.length);
		this.parents = new String[size][];
		for(int i = 0 ; i < size ; i++){
			int s = in.readInt();
			this.parents[i] = new String[s];
			for(int j = 0 ; j < s ; j++){
				get.readFields(in);
				this.parents[i][j] = get.toString();
			}
		}
		
		//writing the CPD.
		size = in.readInt();
		this.CPD = new double[size][];
		for(int i = 0 ; i < size ; i++){
			int s = in.readInt();
			this.CPD[i] = new double[s];
			for(int j = 0 ; j < s; j++){
				this.CPD[i][j] = in.readDouble();
			}
		}
	}
	@Override
	public int compareTo(Node o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
}
