package Tests;

import java.io.*;
import java.util.*;

public class EvaluateMR3 {

	
	ImportGoldStandard gold;
	ArrayList<String> processedRecords;
	private HashSet<String> forbidden=new HashSet<String>();
	
	class MR3Output{
		boolean high;	//if true, then high probability, otherwise 'probable' (not low)
		int score;
		int r1;	//with reference to processedRecords
		int r2;
	}
	
	ArrayList<MR3Output> output;
	
	public EvaluateMR3(String MR3OutputFile, String recordsFile, String goldStandard)
	throws IOException{
		gold=new ImportGoldStandard(recordsFile,goldStandard,1);
		processedRecords=new ArrayList<String>();
		Scanner in=new Scanner(new File(recordsFile));
		
		while(in.hasNextLine()){
			String line=in.nextLine().toLowerCase();
			 String[] res=line.split("\"");
			  if(res.length>1){
			   	String total="";
				for(int j=1; j<res.length; j+=2)
					res[j]=res[j].replace(",","");
				for(int p=0; p<res.length; p++)
					total+=res[p];
				line=total;
						
			  }
			  processedRecords.add(line);
		}
		
		in.close();
		
		
		in=new Scanner(new File(MR3OutputFile));
		output=new ArrayList<MR3Output>();
		
		while(in.hasNextLine()){
			String line=in.nextLine();
			if(forbidden.contains(line))
				continue;
			else
				forbidden.add(line);
			String[] lines=line.split("\t");
			if(lines.length!=3)
				System.out.println("Line Error!");
			String[] lines1=lines[0].split(" ");
			MR3Output p=new MR3Output();
			if(lines1[0].equals("High"))
				p.high=true;
			else
				p.high=false;
			p.score=Integer.parseInt(lines1[1]);
			p.r1=processedRecords.indexOf(lines[1]);
			p.r2=processedRecords.indexOf(lines[2]);
			output.add(p);
			
		}
		
		in.close();
		evaluateResults();
	}
	
	public void evaluateResults(){
		//Note: we ignore probables in this evaluation. Just do among the 'high's.
		int tp=0;
		int total=0;
		for(int i=0; i<output.size(); i++){
			if(!output.get(i).high || output.get(i).score<55)
				continue;
				total++;
			if(gold.contains(output.get(i).r1,output.get(i).r2))
				tp++;
		}
		System.out.println(tp+" Precision: "+tp*1.0/total);
		
	}
	
	public static void main(String[] args) throws IOException{
		String path="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/restaurant/";
		EvaluateMR3 a=new EvaluateMR3(path+"restresult",path+"restaurantRecords.csv",path+"GoldStandard.csv");
	}
	
}
