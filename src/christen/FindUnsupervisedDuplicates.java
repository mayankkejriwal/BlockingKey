package christen;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class FindUnsupervisedDuplicates {
	
	HashMap<Integer,Integer> duplicate_records;
	ArrayList<HashSet<Integer>> clusters;	//we don't do an index thing here
	int num_dups=0; //the total number of duplicate pairs present
	
	
	private ArrayList<Integer> dup1;
	ArrayList<Integer> dup2;
	
	private ArrayList<Integer> nondup1;
	ArrayList<Integer> nondup2;
	
	
			
	char dataset; //currently we support three datasets 'r' 'c' and 's' (restaurant, cora, census)
	
	public FindUnsupervisedDuplicates(char dataset){
		this.dataset=dataset;
	}
	
	public FindUnsupervisedDuplicates(String gold, char dataset) throws IOException{
		this.dataset=dataset;
		importGoldStandard(gold);
		
	}
	
	public void importGoldStandard(String file) throws IOException{
		Scanner in=new Scanner(new File(file));	//the gold standard file
		if(dataset=='c'){
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
			duplicate_records=new HashMap<Integer,Integer>();
			while(in.hasNextLine()){
				String[] d=in.nextLine().split(" ");
				duplicate_records.put(Integer.valueOf(d[0]), Integer.valueOf(d[1]));
			}
			num_dups=duplicate_records.size();
		}
		
		
		in.close();
	}
	
	public void findDuplicates(String duplicateRecords)throws IOException{
		Unsupervised a=new Unsupervised(duplicateRecords);
		if(dataset=='s') //census and record linkage datasets treated differently
			a.computeDuplicatesCensus(Parameters.dupDemanded,Parameters.nondupDemanded,Parameters.ut,Parameters.lt,null);
		else
			a.computeDuplicates_optim(Parameters.dupDemanded,Parameters.nondupDemanded,Parameters.ut,Parameters.lt,null);
		setDup1(a.dup1);
		dup2=a.dup2;
		setNondup1(a.nondup1);
		nondup2=a.nondup2;
		a=null;
	
	}
	
	public boolean contains(int index1,int index2){
		if(dataset=='c')
			return clusterContains(index1,index2);
		
		if(duplicate_records.containsKey(index1))
			if((int) duplicate_records.get(index1)==(int) index2)
				return true;
			else
				return false;
		else if( duplicate_records.containsKey(index2))
			if((int) duplicate_records.get(index2)== (int) index1)
				return true;
			else
				return false;
		else
			return false;
	}
	
	public void printPrecisions(){
		if(dataset=='c')
		{
			clusterPrintPrecisions();
			return;
		}
		int count=0;
		for(int i=0; i<getDup1().size(); i++){
			int min=getDup1().get(i);
			int max=dup2.get(i);
			if(max<min)
				{min=dup2.get(i);
				max=getDup1().get(i);
				}
			if(duplicate_records.containsKey(min))
				if((int) duplicate_records.get(min)==(int) max)
					count++;
		}
		System.out.println("Duplicate precision: "+count*1.0/getDup1().size());
		
		count=0;
		for(int i=0; i<getNondup1().size(); i++){
			int min=getNondup1().get(i);
			int max=nondup2.get(i);
			if(max<min)
				{min=nondup2.get(i);
				max=getNondup1().get(i);
				}
			if(!duplicate_records.containsKey(min))
				count++;
			else if(duplicate_records.containsKey(min))
				if((int) duplicate_records.get(min)!= (int) max)
					count++;
		}
		System.out.println("Non-Duplicate precision: "+count*1.0/getNondup1().size());
	}
	
	public double[] printPrecisionsInArray(){
		if(dataset=='c')
			return clusterPrintPrecisionsInArray();
		double[] result=new double[2];
		int count=0;
		for(int i=0; i<getDup1().size(); i++){
			int min=getDup1().get(i);
			int max=dup2.get(i);
			if(max<min)
				{min=dup2.get(i);
				max=getDup1().get(i);
				}
			if(duplicate_records.containsKey(min))
				if((int) duplicate_records.get(min)==(int) max)
					count++;
		}
		System.out.println("Duplicate precision: "+count*1.0/getDup1().size());
		result[0]=count*1.0/getDup1().size();
		
		count=0;
		for(int i=0; i<getNondup1().size(); i++){
			int min=getNondup1().get(i);
			int max=nondup2.get(i);
			if(max<min)
				{min=nondup2.get(i);
				max=getNondup1().get(i);
				}
			if(!duplicate_records.containsKey(min))
				count++;
			else if(duplicate_records.containsKey(min))
				if((int) duplicate_records.get(min)!= (int) max)
					count++;
		}
		System.out.println("Non-Duplicate precision: "+count*1.0/getNondup1().size());
		result[1]=count*1.0/getNondup1().size();
		return result;
	}
	
	//cora and other cluster based datasets are treated differently
	private boolean clusterContains(int index1, int index2){
		for(int i=0; i<clusters.size(); i++)
			if(clusters.get(i).contains(index1)&&clusters.get(i).contains(index2))
				return true;
			
		return false;
	}
	
	private void clusterPrintPrecisions(){
		int count=0;
		for(int i=0; i<getDup1().size(); i++)
			if(contains(getDup1().get(i),dup2.get(i)))
				count++;
		System.out.println("Count dup1.size() "+count+" "+getDup1().size());
		System.out.println("Duplicate precision: "+count*1.0/getDup1().size());
		
		count=0;
		for(int i=0; i<getNondup1().size(); i++)
			if(!contains(getNondup1().get(i),nondup2.get(i)))
				count++;
		
		System.out.println("Non-Duplicate precision: "+count*1.0/getNondup1().size());
	}
	
	private double[] clusterPrintPrecisionsInArray(){
		double[] result=new double[2];
		int count=0;
		for(int i=0; i<getDup1().size(); i++)
			if(contains(getDup1().get(i),dup2.get(i)))
				count++;
		System.out.println("Count dup1.size() "+count+" "+getDup1().size());
		System.out.println("Duplicate precision: "+count*1.0/getDup1().size());
		result[0]=count*1.0/getDup1().size();
		
		count=0;
		for(int i=0; i<getNondup1().size(); i++)
			if(!contains(getNondup1().get(i),nondup2.get(i)))
				count++;
		
		System.out.println("Non-Duplicate precision: "+count*1.0/getNondup1().size());
		result[1]=count*1.0/getNondup1().size();
		return result;
	}

	public ArrayList<Integer> getNondup1() {
		return nondup1;
	}

	public void setNondup1(ArrayList<Integer> nondup1) {
		this.nondup1 = nondup1;
	}

	public ArrayList<Integer> getDup1() {
		return dup1;
	}

	public void setDup1(ArrayList<Integer> dup1) {
		this.dup1 = dup1;
	}
	
	
	
	

}
