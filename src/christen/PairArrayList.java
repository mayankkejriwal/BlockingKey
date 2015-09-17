package christen;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;



public class PairArrayList {
	//stores an arraylist of pairs. Ordering of pairs is irrelevant
	ArrayList<Integer> dup1=new ArrayList<Integer>();
	ArrayList<Integer> dup2=new ArrayList<Integer>();
	HashMap<Integer,HashSet<Integer>> pairs=new HashMap<Integer,HashSet<Integer>>();
	
	//won't add if it already exists!
	public void add(int integer, int integer2){
		if(exists(integer,integer2))
			return;
		dup1.add(integer);
		dup2.add(integer2);
		if(pairs.containsKey(integer))
			pairs.get(integer).add(integer2);
		else {
			pairs.put(integer,new HashSet<Integer>());
			pairs.get(integer).add(integer2);
		}
	}
	
	public boolean exists(int a, int b){
		if(pairs.containsKey(a))
			if(pairs.get(a).contains(b))
				return true;
		
		if(pairs.containsKey(b))
			if(pairs.get(b).contains( a))
				return true;
		
		return false;
	}
	
	//the unioned structure is the one that's calling it. a will be unmodified
	public void combine(PairArrayList a){

		for(int i=0; i<a.dup1.size(); i++)
			
				add(a.dup1.get(i),a.dup2.get(i));
		
	}
	
	public void printTuples(PrintWriter kk,TFIDF a) throws IOException{
		for(int i=0; i<dup1.size(); i++){
			kk.println(a.getTuples().get(dup1.get(i)));
			kk.println(a.getTuples().get(dup2.get(i)));
			kk.println();
		}
	}
	
	
	
}
