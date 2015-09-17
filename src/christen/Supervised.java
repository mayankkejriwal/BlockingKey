package christen;

import java.util.*;
import java.io.*;

public class Supervised {

	
	
	ArrayList<Integer> split1_traindup1;
	ArrayList<Integer> split1_traindup2;
	
	ArrayList<Integer> split2_traindup1;
	ArrayList<Integer> split2_traindup2;
	
	ArrayList<Integer> split1_trainnondup1;
	ArrayList<Integer> split1_trainnondup2;
	
	ArrayList<Integer> split2_trainnondup1;
	ArrayList<Integer> split2_trainnondup2;
	
	public static int d1=3063;
	public static int d2=14121;
	public static int nd1=5000;
	public static int nd2=791266;
 	
	//if c is 'r' then restaurant, 's' for census, otherwise cora
	//gold is gold standard file
	@SuppressWarnings("unchecked")
	public Supervised(String gold, char c)throws IOException{
		ArrayList<Integer> fulldup1=new ArrayList<Integer>();
		ArrayList<Integer> fulldup2=new ArrayList<Integer>();
		ArrayList<Integer> fullnondup1=new ArrayList<Integer>();
		ArrayList<Integer> fullnondup2=new ArrayList<Integer>();
		ArrayList<HashSet<Integer>> clusters=new ArrayList<HashSet<Integer>>();
		
		if(c!='c'){
			
			FindUnsupervisedDuplicates m=new FindUnsupervisedDuplicates(gold, c);
			
			for(int i:m.duplicate_records.keySet()){
				fulldup1.add(i);
				fulldup2.add(m.duplicate_records.get(i));
				HashSet<Integer> d=new HashSet<Integer>();
				d.add(i);
				d.add(m.duplicate_records.get(i));
				clusters.add(d);
			}
			
			
			
			
			
		}
		
		else{
			FindUnsupervisedDuplicates m=new FindUnsupervisedDuplicates(gold, c);
			for(int i=0;i<m.clusters.size(); i++){
				ArrayList<Integer> tmp=new ArrayList<Integer>(m.clusters.get(i));
				Collections.sort(tmp);
				for(int j=0; j<tmp.size()-1; j++)
					for(int k=j+1; k<tmp.size(); k++){
						fulldup1.add(tmp.get(j));
						fulldup2.add(tmp.get(k));
					}
				
									
			}
			clusters=m.clusters;
		}
			
			for(int i=0; i<clusters.size()-1; i++){
				
				
				for(int j=i+1; j<clusters.size(); j++){
						if(HashSetIntersect(clusters.get(i),clusters.get(j)))
							continue;
						Iterator<Integer> a1=clusters.get(i).iterator();
						
						while(a1.hasNext()){
							int element=a1.next();
							Iterator<Integer> a2=clusters.get(j).iterator();
							while(a2.hasNext()){
								fullnondup1.add(element);
								fullnondup2.add(a2.next());
							}
						}
						
						
				}
			
			}
		
		
		System.out.println(fullnondup1.size());
		populate(fulldup1,fulldup2,fullnondup1,fullnondup2);
		
	}
	
	private boolean HashSetIntersect(HashSet<Integer> a,HashSet<Integer> b){
		if(a==null||b==null)
			return false;
		Iterator<Integer> q=a.iterator();
		while(q.hasNext()){
			if(b.contains(q.next()))
				return true;
			
		}
		return false;
	} 
	
	
	@SuppressWarnings("unchecked")
	private void populate(ArrayList<Integer>...p){
		if(p.length!=4){
			System.out.println("Fatal error! Aborting");
			return;
		}
		populateTrainDupSplits(p[0],p[1],d1,d2);
		populateTrainNonDupSplits(p[2],p[3],nd1,nd2);
		System.out.println("traindup: "+split1_traindup1.size());
		System.out.println("testdup: "+split2_traindup1.size());
		System.out.println("trainnondup: "+split1_trainnondup1.size());
		System.out.println("testnondup: "+split2_trainnondup1.size());
	}
	
	private void populateTrainDupSplits(ArrayList<Integer> full1,ArrayList<Integer> full2, int split1, int split2){
		if(full1.size()!=split1+split2)
		{
			System.out.println("ERRORS: Training set parameters incomplete");
			return;
		}
		split1_traindup1=new ArrayList<Integer>();
		split1_traindup2=new ArrayList<Integer>();
		split2_traindup1=new ArrayList<Integer>();
		split2_traindup2=new ArrayList<Integer>();
		
		
		Random r=new Random(43242423);
		for(int i=0; i<split1;i++){
			int q=r.nextInt(full1.size());
			split1_traindup1.add(full1.get(q));
			split1_traindup2.add(full2.get(q));
			full1.remove(q);
			full2.remove(q);
			
		}
			
		split2_traindup1=full1;
		split2_traindup2=full2;
		
	}
	
	private void populateTrainNonDupSplits(ArrayList<Integer> full1,ArrayList<Integer> full2, int split1, int split2){
		//System.out.println(full1.size()+"  "+full2.size());
		split1_trainnondup1=new ArrayList<Integer>();
		split1_trainnondup2=new ArrayList<Integer>();
		if(full1.size()!=split1+split2)
		{
			System.out.println("ERRORS: Training (non-dup) set parameters incomplete");
			return;
		}
		
		
		Random r=new Random(43242423);
		for(int i=0; i<split1;i++){
			int q=r.nextInt(full1.size());
			split1_trainnondup1.add(full1.get(q));
			split1_trainnondup2.add(full2.get(q));
			full1.remove(q);
			full2.remove(q);
			
		}
			
		split2_trainnondup1=full1;
		split2_trainnondup2=full2;
		
	}
	
	
	
	

}
