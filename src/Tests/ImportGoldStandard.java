package Tests;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

import christen.TFIDF;

public class ImportGoldStandard {

	HashMap<Integer,Integer> duplicate_records; //no transitive closure complications allowed: only disjoint duplicates!
	ArrayList<HashSet<Integer>> clusters;	//we don't do an index thing here
	public int num_dups=0; //the total number of duplicate pairs present
	public int total_pairs=0;
	public TFIDF data;
	boolean cluster_gold;
	//For TFIDF, we assume schema line is missing. 
	   // if opt is 1, then clustering otherwise ordinary pairing
		public  ImportGoldStandard(String dataset, String goldfile, int opt) throws IOException{
			data=new TFIDF(dataset, false);
			total_pairs=data.corpussize*(data.corpussize-1)/2;
			Scanner in=new Scanner(new File(goldfile));	//the gold standard file
			
			if(opt==1){
				cluster_gold=true;
				clusters=new ArrayList<HashSet<Integer>>();
				while(in.hasNextLine()){
					String[] d=in.nextLine().split(" ");
					HashSet<Integer> tmp=new HashSet<Integer>();
					for(int i=0; i<d.length; i++)
						tmp.add(Integer.parseInt(d[i]));
					clusters.add(tmp);
				}
				for(int i=0; i<clusters.size(); i++)
					num_dups+=(clusters.get(i).size()*(clusters.get(i).size()-1)*0.5);
			}
			else{
				cluster_gold=false;
				duplicate_records=new HashMap<Integer,Integer>();
				while(in.hasNextLine()){
					String[] d=in.nextLine().split(" ");
					duplicate_records.put(Integer.valueOf(d[0]), Integer.valueOf(d[1]));
				}
				num_dups=duplicate_records.size();
			}
			
			
			in.close();
		}
		
		public boolean contains(int i, int j){
			if(!cluster_gold){
				if(duplicate_records.containsKey(i))
					if((int)duplicate_records.get(i)==j)
						return true;
					else return false;
				else if(duplicate_records.containsKey(j))
					if((int)duplicate_records.get(j)==i)
						return true;
					else return false;
				else
					return false;
			}
			else{
				for(int p=0; p<clusters.size(); p++)
					if(clusters.get(p).contains(i)&&clusters.get(p).contains(j))
						return true;
				return false;
			}
		}
	
}
