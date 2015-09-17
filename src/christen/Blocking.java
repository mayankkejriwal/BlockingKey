package christen;

import java.io.IOException;
import java.util.*;




public class Blocking {
	
	int[] attr;
	TFIDF db;
	
	
	HashMap<Integer,HashMap<String,ArrayList<Integer>>> cblocks; //column, >cvalue blocks
	ArrayList<HashMap<String,ArrayList<Integer>>> bkv_list; //outer array list corresponds to attr
	private ArrayList<String[]> BKVs=new ArrayList<String[]>(); //wrt attr
	
	
	//by default block on all attributes
	public Blocking(TFIDF a) throws IOException{
		db=a;
		db.calculateweights();
		int att=db.numatts;
		
		attr=new int[att];
		for(int i=0; i<att; i++)
			attr[i]=i;
		
		
	}
	
	
	
	
	//keep db around
	public void memop(){
		
		attr=null;
		bkv_list=null;
		cblocks=null;
		BKVs=null;
	}
	
	
	public void populateSimpleBKs(){
		ArrayList<String> base=db.getTuples();
		
		for(int i=0; i<base.size(); i++){
			String[] tuple=base.get(i).split(",");
			String[] p=new String[attr.length];
			
			for(int j=0; j<attr.length; j++)	//for handling missing entries in the end
				if(attr[j]<tuple.length)
				p[j]=tuple[attr[j]];
				else
					p[j]="";
			BKVs.add(p);
			
		}
		
		
		hashList();	//within attribute parsing supported
		
		identifyCBlocks();
		
	}
	
	
	/*
	 * Some helpful printing functions
	 */
	public void print_bkvlist(){
		
		for(int i=0; i<bkv_list.size(); i++)
		{
			System.out.println("A new Blocking Key");
			printbkv(bkv_list.get(i));
		}
		
	}

	public void printbkv(HashMap<String,ArrayList<Integer>> bkv){
		for(String i: bkv.keySet()){
			System.out.println("BKV: "+i);
			System.out.println(bkv.get(i).size());
			for(int j=0; j<bkv.get(i).size(); j++)
				System.out.println(db.getTuples().get(bkv.get(i).get(j)));
		}
	}

	//this would check for these c blocks in  bkv_list 
	private void identifyCBlocks(){
		cblocks=new HashMap<Integer,HashMap<String,ArrayList<Integer>>>();
		
		if(bkv_list!=null && bkv_list.size()!=0){
			
			for(int i=0; i<bkv_list.size(); i++)
				for(String j: bkv_list.get(i).keySet())
					if(bkv_list.get(i).get(j).size()>Parameters.c){
						ArrayList<Integer> q=bkv_list.get(i).get(j);
						if(cblocks.containsKey(i))
							cblocks.get(i).put(j,q);
						else{
							cblocks.put(i, new HashMap<String,ArrayList<Integer>>());
							cblocks.get(i).put(j,q);
						}
						//bkv_list.get(i).remove(j);
					}
		}
		
		
		
		
	}

	private void hashList(){
		ArrayList<ArrayList<String>> p=new ArrayList<ArrayList<String>>(BKVs.get(0).length);
		
		for(int i=0; i<BKVs.get(0).length; i++)
			p.add(new ArrayList<String>());
		
		for(int i=0; i<BKVs.size(); i++)
			for(int j=0; j<BKVs.get(0).length; j++)
				p.get(j).add(BKVs.get(i)[j]);
			
		bkv_list=new ArrayList<HashMap<String, ArrayList<Integer>>>();
		
		for(int i=0; i<p.size(); i++)
			bkv_list.add(hashBKV(p.get(i)));
		
	}
	
	//supports parsing; returns set of blocks
	private HashMap<String,ArrayList<Integer>> hashBKV(ArrayList<String> bkvcol){
		
		
		HashMap<String,HashSet<Integer>> result=new HashMap<String,HashSet<Integer>>();
		for(int i=0; i<bkvcol.size(); i++){
			
			
			ArrayList<String> q=lexordering(bkvcol.get(i).split(Parameters.splitstring));
			
			for(int k=0; k<q.size(); k++)
				
			if(result.containsKey(q.get(k)))
				result.get(q.get(k)).add(i);
			else{
				result.put(q.get(k), new HashSet<Integer>());
				result.get(q.get(k)).add(i);
				
			}
			
			
		}
		HashMap<String,ArrayList<Integer>> finresult=new HashMap<String,ArrayList<Integer>>();
		for(String i: result.keySet()){
			finresult.put(i, new ArrayList<Integer>(result.get(i)));
		}
		return finresult;
		
	}
	
	//also considers comma delimited 'total' string in the ordering
	private ArrayList<String> lexordering(String[] tokens){
		ArrayList<String> q=new ArrayList<String>(tokens.length+1);
		String total="";
		for(int i=0; i<tokens.length; i++)
			if(tokens[i].trim().length()!=0){
				q.add(tokens[i]);
				total=total+tokens[i]+",";
			}
		if(total.length()!=0)
		q.add(total.substring(0,total.length()-1));
		Collections.sort(q);
		return q;
	}
	

}
