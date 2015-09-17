package Tests;

import java.io.*;
import java.text.DecimalFormat;
import java.util.ArrayList;

import christen.DNFBlocking;
import christen.FeatureGen;
import christen.FindUnsupervisedDuplicates;
import christen.Parameters;
import christen.Supervised;
import christen.Unsupervised;

import FeatureSelection.FeatureAnalysis;
import FeatureSelection.Fisher;
import FeatureSelection.LearnDisjunct;
import MapReduce.MRClass;

public class Experiments {
	static int conjuncts=2;
	static int c=20; //Parameters.c
	static int r_att=4;
	static int c_att=13;
	static int s_att=7;
	static int num_feats=25;
	
	static String rPath="/home/mayankkejriwal/Documents/datasets/restaurant/";
	static String cPath="/home/mayankkejriwal/Documents/datasets/cora/";
	static String sPath="/home/mayankkejriwal/Documents/datasets/census/";
	
	//supervised params
	static int S_r_d1=56;
	static int S_r_d2=56;
	static int S_r_nd1=1000;
	static int S_r_nd2=23864;
	
	static int S_c_d1=3063;
	static int S_c_d2=14121;
	static int S_c_nd1=1000;
	static int S_c_nd2=795266;
	
	static int S_s_d1=163;
	static int S_s_d2=164;
	static int S_s_nd1=1002;
	static int S_s_nd2=212202;
	
	//unsupervised params
	static double U_r_ut=0.08;
	static double U_r_lt=0.005;
	static int U_r_d=56;
	static int U_r_nd=1000;
	
	static double U_c_ut=0.08;
	static double U_c_lt=0.005;
	static int U_c_d=3063;
	static int U_c_nd=1000;
	
	static double U_s_ut=0.08;
	static double U_s_lt=0.005;
	static int U_s_d=163;
	static int U_s_nd=1002;
	
	static String MR_file_r="/home/mayankkejriwal/Downloads/hadoop_programs/restaurantMR_1";
	static String MR_file_c="/home/mayankkejriwal/Downloads/hadoop_programs/cora_MR";
	static String MR_file_s="/home/mayankkejriwal/Downloads/hadoop_programs/census_MR";
	
	public static void main(String[] args) throws IOException {
		
		Phase1Full('r',rPath+"/Phase1_rename.txt");
		//UnSupervisedDNFFull('f','c',cPath+"DNF/uf_3063.txt");
		//UnSupervisedDNFFull('b','c',cPath+"DNF/ub_3063.txt");
		//SupervisedDNFFull('b','c',cPath+"DNF/sb_3063.txt");
		//UnSupervisedDNFFull('f','s',sPath+"DNF/uf_163_2.txt");
		
		//UnSupervisedDisjunctFull('f','r',rPath+"Disjunction/test1.txt");
		//MRDisjunctFull('f','c',cPath+"Disjunction/MR_test.txt",MR_file_c);
		
	}
	
	public static void MRDisjunctFull(char method, char dataset, String file, String inputfile)throws IOException{
		Parameters.DNF=false;
		if(dataset=='r')
		{
			Parameters.ut=U_r_ut;
			Parameters.lt=U_r_lt;
			Parameters.dupDemanded=U_r_d;
			Parameters.nondupDemanded=U_r_nd;
		}
		else if(dataset=='c'){
			Parameters.ut=U_c_ut;
			Parameters.lt=U_c_lt;
			Parameters.dupDemanded=U_c_d;
			Parameters.nondupDemanded=U_c_nd;
		}
		else if(dataset=='s'){
			Parameters.ut=U_s_ut;
			Parameters.lt=U_s_lt;
			Parameters.dupDemanded=U_s_d;
			Parameters.nondupDemanded=U_s_nd;
		}
		PrintWriter out=new PrintWriter(new File(file));
		out.println(Parameters.ut+" "+Parameters.lt+" DD: "+Parameters.dupDemanded+" NDD: "+Parameters.nondupDemanded);
		DecimalFormat p=new DecimalFormat("0.00");
		DecimalFormat q=new DecimalFormat("0.000000");
		FeatureAnalysis d=null;
		
		
		d=MRClass.getMRResults(inputfile,dataset);
		Parameters.eta=0.0;
		out.println("eta=1"+"\n\n");
		out.println("Recall\tRR\t\tPC\tPC\t\tPQ\t\tBKs");
		for(double i=0.0; i<=1.0; i+=0.1){
			Dataset(i, method, d, dataset);
			out.print(p.format(i)+"\t");
			out.println(q.format(DNFBlocking.RR)+"\t"+DNFBlocking.PCount+"\t"+q.format(DNFBlocking.Pcomp)+"\t"+q.format(DNFBlocking.PQ)+"\t"+DNFBlocking.BKs);
		}
		out.println("\n\n");
		
		
		Parameters.eta=0.1;
		out.println("eta=10%"+"\n\n");
		out.println("Recall\tRR\t\tPC\tPC\t\tPQ\t\tBKs");
		for(double i=0.0; i<=1.0; i+=0.1){
			Dataset(i, method, d, dataset);
			out.print(p.format(i)+"\t");
			out.println(q.format(DNFBlocking.RR)+"\t"+DNFBlocking.PCount+"\t"+q.format(DNFBlocking.Pcomp)+"\t"+q.format(DNFBlocking.PQ)+"\t"+DNFBlocking.BKs);
		}
		out.println("\n\n");
		
		
		Parameters.eta=0.5;
		out.println("eta=50%"+"\n\n");
		out.println("Recall\tRR\t\tPC\tPC\t\tPQ\t\tBKs");
		for(double i=0.0; i<=1.0; i+=0.1){
			Dataset(i, method, d, dataset);
			out.print(p.format(i)+"\t");
			out.println(q.format(DNFBlocking.RR)+"\t"+DNFBlocking.PCount+"\t"+q.format(DNFBlocking.Pcomp)+"\t"+q.format(DNFBlocking.PQ)+"\t"+DNFBlocking.BKs);
		}
		out.println("\n\n");
		
		
		Parameters.eta=1.0;
		out.println("eta=100%"+"\n\n");
		out.println("Recall\tRR\t\tPC\tPC\t\tPQ\t\tBKs");
		for(double i=0.0; i<=1.0; i+=0.1){
			Dataset(i, method, d, dataset);
			out.print(p.format(i)+"\t");
			out.println(q.format(DNFBlocking.RR)+"\t"+DNFBlocking.PCount+"\t"+q.format(DNFBlocking.Pcomp)+"\t"+q.format(DNFBlocking.PQ)+"\t"+DNFBlocking.BKs);
		}
		out.println("\n\n");
		
		out.close();
	}
	
	
	public static void Phase1Full(char dataset, String file)throws IOException{
		PrintWriter out=new PrintWriter(new File(file));
		
		DecimalFormat q=new DecimalFormat("0.000000");
		DecimalFormat p=new DecimalFormat("0.000");
		
		if(dataset=='r'){
			Parameters.nondupDemanded=1000;
			Parameters.ut=0.08;
			Parameters.lt=0.01;
			Parameters.c=20;
			out.println("DD: Duplicated Demanded; DR: Duplicates Retrieved; DP: Duplicate Precision; NDD: Non-Duplicates Demanded...etc.");
			out.println();
			out.println("Parameters: UT: "+Parameters.ut+" LT: "+Parameters.lt+" c: "+Parameters.c);
			
			out.println("DD\tDR\tDP\t\tNDD\tNDR\tNDP");
			double[] result=null;
			//first
			for(Parameters.dupDemanded=50; Parameters.dupDemanded<=100; Parameters.dupDemanded=Parameters.dupDemanded + 10){
				FindUnsupervisedDuplicates rest=new FindUnsupervisedDuplicates('r');
				rest.importGoldStandard(rPath+"GoldStandard.csv");
				rest.findDuplicates(rPath+"restaurantRecords.csv");
				result=rest.printPrecisionsInArray();
				out.print(Parameters.dupDemanded+"\t"+rest.getDup1().size()+"\t"+q.format(result[0])+"\t");
				out.println(Parameters.nondupDemanded+"\t"+rest.getNondup1().size()+"\t"+q.format(result[1]));
				
			}
			//second
			Parameters.dupDemanded=50;
			out.println();
			out.println("Parameters: UT: "+Parameters.ut+" LT: "+Parameters.lt+" c: "+Parameters.c);
			out.println("DD\tDR\tDP\t\tNDD\tNDR\tNDP");
			for(Parameters.nondupDemanded=1000; Parameters.nondupDemanded<=21000; Parameters.nondupDemanded=Parameters.nondupDemanded + 4000){
				FindUnsupervisedDuplicates rest=new FindUnsupervisedDuplicates('r');
				rest.importGoldStandard(rPath+"GoldStandard.csv");
				rest.findDuplicates(rPath+"restaurantRecords.csv");
				result=rest.printPrecisionsInArray();
				out.print(Parameters.dupDemanded+"\t"+rest.getDup1().size()+"\t"+q.format(result[0])+"\t");
				out.println(Parameters.nondupDemanded+"\t"+rest.getNondup1().size()+"\t"+q.format(result[1]));
				
			}
			//third
			Parameters.dupDemanded=50;
			Parameters.nondupDemanded=1000;
			out.println();
			out.println("Parameters: DD, DR: "+Parameters.dupDemanded+" NDD: "+Parameters.nondupDemanded+" UT: "+Parameters.ut+" c: "+Parameters.c);
			out.println("LT\tNDR\tNDP");
			for(Parameters.lt=0.005; Parameters.lt<=0.1; Parameters.lt=Parameters.lt + 0.005){
				FindUnsupervisedDuplicates rest=new FindUnsupervisedDuplicates('r');
				rest.importGoldStandard(rPath+"GoldStandard.csv");
				rest.findDuplicates(rPath+"restaurantRecords.csv");
				result=rest.printPrecisionsInArray();
				out.println(p.format(Parameters.lt)+"\t"+rest.getNondup1().size()+"\t"+q.format(result[1]));
				//out.println(DataCollectionRestaurant.non_duplicates+"\t"+rest.nondup1.size()+"\t"+q.format(result[1]));
				
			}
			//fourth
			Parameters.dupDemanded=50;
			Parameters.nondupDemanded=1000;
			Parameters.lt=0.01;
			out.println();
			out.println("Parameters: NDD, NDR: "+Parameters.nondupDemanded+" DD: "+Parameters.dupDemanded+" LT: "+Parameters.lt+" c: "+Parameters.c);
			out.println("UT\tDR\tDP");
			for(Parameters.ut=0.005; Parameters.ut<=0.1; Parameters.ut=Parameters.ut + 0.005){
				FindUnsupervisedDuplicates rest=new FindUnsupervisedDuplicates('r');
				rest.importGoldStandard(rPath+"GoldStandard.csv");
				rest.findDuplicates(rPath+"restaurantRecords.csv");
				result=rest.printPrecisionsInArray();
				out.println(p.format(Parameters.ut)+"\t"+rest.getDup1().size()+"\t"+q.format(result[0]));
				//out.println(DataCollectionRestaurant.non_duplicates+"\t"+rest.nondup1.size()+"\t"+q.format(result[1]));
				
			}
			//fifth
			Parameters.dupDemanded=100;
			Parameters.nondupDemanded=1000;
			Parameters.lt=0.01;
			out.println();
			out.println("Parameters: NDD, NDR: "+Parameters.nondupDemanded+" DD: "+Parameters.dupDemanded+" LT: "+Parameters.lt+" c: "+Parameters.c);
			out.println("UT\tDR\tDP");
			for(Parameters.ut=0.005; Parameters.ut<=0.1; Parameters.ut=Parameters.ut + 0.005){
				FindUnsupervisedDuplicates rest=new FindUnsupervisedDuplicates('r');
				rest.importGoldStandard(rPath+"GoldStandard.csv");
				rest.findDuplicates(rPath+"restaurantRecords.csv");
				result=rest.printPrecisionsInArray();
				out.println(p.format(Parameters.ut)+"\t"+rest.getDup1().size()+"\t"+q.format(result[0]));
				//out.println(DataCollectionRestaurant.non_duplicates+"\t"+rest.nondup1.size()+"\t"+q.format(result[1]));
				
			}
			//sixth
			Parameters.dupDemanded=100;
			Parameters.nondupDemanded=1000;
			Parameters.lt=0.01;
			Parameters.ut=0.08;
			out.println();
			out.println("Parameters: NDD, DD: "+Parameters.nondupDemanded+" "+Parameters.dupDemanded+" LT: "+Parameters.lt+" UT: "+Parameters.ut);
			out.println("c\tDR\tDP\t\tNDR\tNDP");
			for(Parameters.c=20; Parameters.c<=50; Parameters.c+=10){
				FindUnsupervisedDuplicates rest=new FindUnsupervisedDuplicates('r');
				rest.importGoldStandard(rPath+"GoldStandard.csv");
				rest.findDuplicates(rPath+"restaurantRecords.csv");
				result=rest.printPrecisionsInArray();
				out.println(Parameters.c+"\t"+rest.getDup1().size()+"\t"+q.format(result[0])+"\t"+rest.getNondup1().size()+"\t"+q.format(result[1]));
				//out.println(DataCollectionRestaurant.non_duplicates+"\t"+rest.nondup1.size()+"\t"+q.format(result[1]));
				
			}
			
		}
		else if(dataset=='c'){
			
			Parameters.nondupDemanded=5000;
			Parameters.ut=0.05;
			Parameters.lt=0.01;
			Parameters.c=20;
			out.println("DD: Duplicated Demanded; DR: Duplicates Retrieved; DP: Duplicate Precision; NDD: Non-Duplicates Demanded...etc.");
			out.println();
			out.println("Parameters: UT: "+Parameters.ut+" LT: "+Parameters.lt+" c: "+Parameters.c);
			
			out.println("DD\tDR\tDP\t\tNDD\tNDR\tNDP");
			double[] result=null;
			//first
			for(Parameters.dupDemanded=50; Parameters.dupDemanded<=2000; Parameters.dupDemanded=Parameters.dupDemanded + 100){
				FindUnsupervisedDuplicates rest=new FindUnsupervisedDuplicates('c');
				rest.importGoldStandard(cPath+"GoldStandard.csv");
				rest.findDuplicates(cPath+"coraRecords.csv");
				result=rest.printPrecisionsInArray();
				out.print(Parameters.dupDemanded+"\t"+rest.getDup1().size()+"\t"+q.format(result[0])+"\t");
				out.println(Parameters.nondupDemanded+"\t"+rest.getNondup1().size()+"\t"+q.format(result[1]));
				
			}
			//second
			Parameters.dupDemanded=500;
			out.println();
			out.println("Parameters: UT: "+Parameters.ut+" LT: "+Parameters.lt+" c: "+Parameters.c);
			out.println("DD\tDR\tDP\t\tNDD\tNDR\tNDP");
			for(Parameters.nondupDemanded=1000; Parameters.nondupDemanded<=21000; Parameters.nondupDemanded=Parameters.nondupDemanded + 4000){
				FindUnsupervisedDuplicates rest=new FindUnsupervisedDuplicates('c');
				rest.importGoldStandard(cPath+"GoldStandard.csv");
				rest.findDuplicates(cPath+"coraRecords.csv");
				result=rest.printPrecisionsInArray();
				out.print(Parameters.dupDemanded+"\t"+rest.getDup1().size()+"\t"+q.format(result[0])+"\t");
				out.println(Parameters.nondupDemanded+"\t"+rest.getNondup1().size()+"\t"+q.format(result[1]));
				
			}
			
			//third
			Parameters.dupDemanded=500;
			Parameters.nondupDemanded=20000;
			Parameters.ut=0.05;
			out.println();
			out.println("Parameters: DD, DR: "+Parameters.dupDemanded+" NDD: "+Parameters.nondupDemanded+" UT: "+Parameters.ut+" c: "+Parameters.c);
			out.println("LT\tNDR\tNDP");
			for(Parameters.lt=0.005; Parameters.lt<=0.1; Parameters.lt=Parameters.lt + 0.005){
				FindUnsupervisedDuplicates rest=new FindUnsupervisedDuplicates('c');
				rest.importGoldStandard(cPath+"GoldStandard.csv");
				rest.findDuplicates(cPath+"coraRecords.csv");
				result=rest.printPrecisionsInArray();
				out.println(p.format(Parameters.lt)+"\t"+rest.getNondup1().size()+"\t"+q.format(result[1]));
				//out.println(DataCollectionRestaurant.non_duplicates+"\t"+rest.nondup1.size()+"\t"+q.format(result[1]));
				
			}
			
			
			//fourth
			Parameters.dupDemanded=2000;
			Parameters.nondupDemanded=1000;
			Parameters.lt=0.01;
			out.println();
			out.println("Parameters: NDD, NDR: "+Parameters.nondupDemanded+" DD: "+Parameters.dupDemanded+" LT: "+Parameters.lt+" c: "+Parameters.c);
			out.println("UT\tDR\tDP");

			for(Parameters.ut=0.005; Parameters.ut<=0.1; Parameters.ut=Parameters.ut + 0.005){
				FindUnsupervisedDuplicates rest=new FindUnsupervisedDuplicates('c');
				rest.importGoldStandard(cPath+"GoldStandard.csv");
				rest.findDuplicates(cPath+"coraRecords.csv");
				result=rest.printPrecisionsInArray();
				out.println(p.format(Parameters.ut)+"\t"+rest.getDup1().size()+"\t"+q.format(result[0]));
				//out.println(DataCollectionRestaurant.non_duplicates+"\t"+rest.nondup1.size()+"\t"+q.format(result[1]));
				
			}
			
			
			
			//fifth
			Parameters.dupDemanded=500;
			Parameters.nondupDemanded=1000;
			Parameters.lt=0.01;
			Parameters.ut=0.03;
			out.println();
			out.println("Parameters: NDD, DD: "+Parameters.nondupDemanded+" "+Parameters.dupDemanded+" LT: "+Parameters.lt+" UT: "+Parameters.ut);
			out.println("c\tDR\tDP\t\tNDR\tNDP");
			for(Parameters.c=20; Parameters.c<=50; Parameters.c+=10){
				FindUnsupervisedDuplicates rest=new FindUnsupervisedDuplicates('c');
				rest.importGoldStandard(cPath+"GoldStandard.csv");
				rest.findDuplicates(cPath+"coraRecords.csv");
				result=rest.printPrecisionsInArray();
				out.println(Parameters.c+"\t"+rest.getDup1().size()+"\t"+q.format(result[0])+"\t"+rest.getNondup1().size()+"\t"+q.format(result[1]));
				//out.println(DataCollectionRestaurant.non_duplicates+"\t"+rest.nondup1.size()+"\t"+q.format(result[1]));
				
			}
		}
		
		else if(dataset=='s'){
			Parameters.nondupDemanded=1000;
			Parameters.ut=0.08;
			Parameters.lt=0.01;
			Parameters.c=20;
			out.println("DD: Duplicated Demanded; DR: Duplicates Retrieved; DP: Duplicate Precision; NDD: Non-Duplicates Demanded...etc.");
			out.println();
			out.println("Parameters: UT: "+Parameters.ut+" LT: "+Parameters.lt+" c: "+Parameters.c);
			
			out.println("DD\tDR\tDP\t\tNDD\tNDR\tNDP");
			double[] result=null;
			//first
			for(Parameters.dupDemanded=50; Parameters.dupDemanded<=200; Parameters.dupDemanded=Parameters.dupDemanded + 10){
				FindUnsupervisedDuplicates rest=new FindUnsupervisedDuplicates('s');
				rest.importGoldStandard(sPath+"GoldStandard.csv");
				rest.findDuplicates(sPath+"censusRecords.csv");
				result=rest.printPrecisionsInArray();
				out.print(Parameters.dupDemanded+"\t"+rest.getDup1().size()+"\t"+q.format(result[0])+"\t");
				out.println(Parameters.nondupDemanded+"\t"+rest.getNondup1().size()+"\t"+q.format(result[1]));
				
			}
			
			//second
			out.println();
			Parameters.dupDemanded=100;
			out.println("Parameters: UT: "+Parameters.ut+" LT: "+Parameters.lt+" c: "+Parameters.c);
			
			out.println("DD\tDR\tDP\t\tNDD\tNDR\tNDP");
			
			
			for(Parameters.nondupDemanded=1000; Parameters.nondupDemanded<=21000; Parameters.nondupDemanded=Parameters.nondupDemanded + 4000){
				FindUnsupervisedDuplicates rest=new FindUnsupervisedDuplicates('s');
				rest.importGoldStandard(sPath+"GoldStandard.csv");
				rest.findDuplicates(sPath+"censusRecords.csv");
				result=rest.printPrecisionsInArray();
				out.print(Parameters.dupDemanded+"\t"+rest.getDup1().size()+"\t"+q.format(result[0])+"\t");
				out.println(Parameters.nondupDemanded+"\t"+rest.getNondup1().size()+"\t"+q.format(result[1]));
				
			}
			
			//third
			Parameters.dupDemanded=100;
			Parameters.nondupDemanded=1000;
			Parameters.ut=0.08;
			out.println();
			out.println("Parameters: DD, DR: "+Parameters.dupDemanded+" NDD: "+Parameters.nondupDemanded+" UT: "+Parameters.ut+" c: "+Parameters.c);
			out.println("LT\tNDR\tNDP");
			for(Parameters.lt=0.005; Parameters.lt<=0.1; Parameters.lt=Parameters.lt + 0.005){
				FindUnsupervisedDuplicates rest=new FindUnsupervisedDuplicates('s');
				rest.importGoldStandard(sPath+"GoldStandard.csv");
				rest.findDuplicates(sPath+"censusRecords.csv");
				result=rest.printPrecisionsInArray();
				out.println(p.format(Parameters.lt)+"\t"+rest.getNondup1().size()+"\t"+q.format(result[1]));
				//out.println(DataCollectionRestaurant.non_duplicates+"\t"+rest.nondup1.size()+"\t"+q.format(result[1]));
				
			}
			

			//fourth
			Parameters.dupDemanded=100;
			Parameters.nondupDemanded=1000;
			Parameters.lt=0.01;
			out.println();
			out.println("Parameters: NDD, NDR: "+Parameters.nondupDemanded+" DD: "+Parameters.dupDemanded+" LT: "+Parameters.lt+" c: "+Parameters.c);
			out.println("UT\tDR\tDP");

			for(Parameters.ut=0.005; Parameters.ut<=0.1; Parameters.ut=Parameters.ut + 0.005){
				FindUnsupervisedDuplicates rest=new FindUnsupervisedDuplicates('s');
				rest.importGoldStandard(sPath+"GoldStandard.csv");
				rest.findDuplicates(sPath+"censusRecords.csv");
				result=rest.printPrecisionsInArray();
				out.println(p.format(Parameters.ut)+"\t"+rest.getDup1().size()+"\t"+q.format(result[0]));
				//out.println(DataCollectionRestaurant.non_duplicates+"\t"+rest.nondup1.size()+"\t"+q.format(result[1]));
				
			}
			
			
			//fifth
			Parameters.dupDemanded=200;
			Parameters.nondupDemanded=1000;
			Parameters.lt=0.01;
			out.println();
			out.println("Parameters: NDD, NDR: "+Parameters.nondupDemanded+" DD: "+Parameters.dupDemanded+" LT: "+Parameters.lt+" c: "+Parameters.c);
			out.println("UT\tDR\tDP");

			for(Parameters.ut=0.005; Parameters.ut<=0.1; Parameters.ut=Parameters.ut + 0.005){
				FindUnsupervisedDuplicates rest=new FindUnsupervisedDuplicates('s');
				rest.importGoldStandard(sPath+"GoldStandard.csv");
				rest.findDuplicates(sPath+"censusRecords.csv");
				result=rest.printPrecisionsInArray();
				out.println(p.format(Parameters.ut)+"\t"+rest.getDup1().size()+"\t"+q.format(result[0]));
				//out.println(DataCollectionRestaurant.non_duplicates+"\t"+rest.nondup1.size()+"\t"+q.format(result[1]));
				
			}
			
			
			
			//sixth
			Parameters.dupDemanded=200;
			Parameters.nondupDemanded=1000;
			Parameters.lt=0.01;
			Parameters.ut=0.08;
			out.println();
			out.println("Parameters: NDD, DD: "+Parameters.nondupDemanded+" "+Parameters.dupDemanded+" LT: "+Parameters.lt+" UT: "+Parameters.ut);
			out.println("c\tDR\tDP\t\tNDR\tNDP");
			for(Parameters.c=20; Parameters.c<=50; Parameters.c+=10){
				FindUnsupervisedDuplicates rest=new FindUnsupervisedDuplicates('s');
				rest.importGoldStandard(sPath+"GoldStandard.csv");
				rest.findDuplicates(sPath+"censusRecords.csv");
				result=rest.printPrecisionsInArray();
				out.println(Parameters.c+"\t"+rest.getDup1().size()+"\t"+q.format(result[0])+"\t"+rest.getNondup1().size()+"\t"+q.format(result[1]));
				//out.println(DataCollectionRestaurant.non_duplicates+"\t"+rest.nondup1.size()+"\t"+q.format(result[1]));
				
			}
		}
		
		out.close();
	}
	
	public static void SupervisedDisjunctFull(char method, char dataset,String file)throws IOException{
		Parameters.DNF=false;
		if(dataset=='r')
		{
			Supervised.d1=S_r_d1;
			Supervised.d2=S_r_d2;
			Supervised.nd1=S_r_nd1;
			Supervised.nd2=S_r_nd2;
		}
		else if(dataset=='c'){
			Supervised.d1=S_c_d1;
			Supervised.d2=S_c_d2;
			Supervised.nd1=S_c_nd1;
			Supervised.nd2=S_c_nd2;
		}
		else if(dataset=='s'){
			Supervised.d1=S_s_d1;
			Supervised.d2=S_s_d2;
			Supervised.nd1=S_s_nd1;
			Supervised.nd2=S_s_nd2;
		}
		
		PrintWriter out=new PrintWriter(new File(file));
		out.println(Supervised.d1+" "+Supervised.nd1);
		DecimalFormat p=new DecimalFormat("0.00");
		DecimalFormat q=new DecimalFormat("0.000000");
		FeatureAnalysis d=null;
		d=SupervisedDatasetHelp(dataset);
		
		Parameters.eta=0.0;
		out.println("eta=1"+"\n\n");
		out.println("Recall\tRR\t\tPC\tPC\t\tPQ\t\tBKs");
		for(double i=0.0; i<=1.0; i+=0.1){
			Dataset(i, method, d, dataset);
			
			out.print(p.format(i)+"\t");
			out.println(q.format(DNFBlocking.RR)+"\t"+DNFBlocking.PCount+"\t"+q.format(DNFBlocking.Pcomp)+"\t"+q.format(DNFBlocking.PQ)+"\t"+DNFBlocking.BKs);
		}
		out.println("\n\n");
		
		
		Parameters.eta=0.1;
		out.println("eta=10%"+"\n\n");
		out.println("Recall\tRR\t\tPC\tPC\t\tPQ\t\tBKs");
		for(double i=0.0; i<=1.0; i+=0.1){
			Dataset(i, method, d, dataset);
			out.print(p.format(i)+"\t");
			out.println(q.format(DNFBlocking.RR)+"\t"+DNFBlocking.PCount+"\t"+q.format(DNFBlocking.Pcomp)+"\t"+q.format(DNFBlocking.PQ)+"\t"+DNFBlocking.BKs);
		}
		out.println("\n\n");
		
		
		Parameters.eta=0.5;
		out.println("eta=50%"+"\n\n");
		out.println("Recall\tRR\t\tPC\tPC\t\tPQ\t\tBKs");
		for(double i=0.0; i<=1.0; i+=0.1){
			Dataset(i, method, d, dataset);
			out.print(p.format(i)+"\t");
			out.println(q.format(DNFBlocking.RR)+"\t"+DNFBlocking.PCount+"\t"+q.format(DNFBlocking.Pcomp)+"\t"+q.format(DNFBlocking.PQ)+"\t"+DNFBlocking.BKs);
		}
		out.println("\n\n");
		
		
		Parameters.eta=1.0;
		out.println("eta=100%"+"\n\n");
		out.println("Recall\tRR\t\tPC\tPC\t\tPQ\t\tBKs");
		for(double i=0.0; i<=1.0; i+=0.1){
			Dataset(i, method, d, dataset);
			out.print(p.format(i)+"\t");
			out.println(q.format(DNFBlocking.RR)+"\t"+DNFBlocking.PCount+"\t"+q.format(DNFBlocking.Pcomp)+"\t"+q.format(DNFBlocking.PQ)+"\t"+DNFBlocking.BKs);
		}
		out.println("\n\n");
		
		out.close();
	}

	public static void SupervisedDNFFull(char method, char dataset,String file)throws IOException{
		Parameters.DNF=true;
		if(dataset=='r')
		{
			Supervised.d1=S_r_d1;
			Supervised.d2=S_r_d2;
			Supervised.nd1=S_r_nd1;
			Supervised.nd2=S_r_nd2;
		}
		else if(dataset=='c'){
			Supervised.d1=S_c_d1;
			Supervised.d2=S_c_d2;
			Supervised.nd1=S_c_nd1;
			Supervised.nd2=S_c_nd2;
		}
		else if(dataset=='s'){
			Supervised.d1=S_s_d1;
			Supervised.d2=S_s_d2;
			Supervised.nd1=S_s_nd1;
			Supervised.nd2=S_s_nd2;
		}
		PrintWriter out=new PrintWriter(new File(file));
		out.println(Supervised.d1+" "+Supervised.nd1);
		DecimalFormat p=new DecimalFormat("0.00");
		DecimalFormat q=new DecimalFormat("0.000000");
		FeatureAnalysis d=null;
		d=SupervisedDatasetHelp(dataset);
		
		Parameters.eta=0.0;
		out.println("eta=1"+"\n\n");
		out.println("Recall\tRR\t\tPC\tPC\t\tPQ\t\tBKs");
		for(double i=0.0; i<=1.0; i+=0.1){
			DatasetDNF(i, method, d, dataset);
			out.print(p.format(i)+"\t");
			out.println(q.format(DNFBlocking.RR)+"\t"+DNFBlocking.PCount+"\t"+q.format(DNFBlocking.Pcomp)+"\t"+q.format(DNFBlocking.PQ)+"\t"+DNFBlocking.BKs);
		}
		out.println("\n\n");
		
		
		Parameters.eta=0.1;
		out.println("eta=10%"+"\n\n");
		out.println("Recall\tRR\t\tPC\tPC\t\tPQ\t\tBKs");
		for(double i=0.0; i<=1.0; i+=0.1){
			DatasetDNF(i, method, d, dataset);
			out.print(p.format(i)+"\t");
			out.println(q.format(DNFBlocking.RR)+"\t"+DNFBlocking.PCount+"\t"+q.format(DNFBlocking.Pcomp)+"\t"+q.format(DNFBlocking.PQ)+"\t"+DNFBlocking.BKs);
		}
		out.println("\n\n");
		
		
		Parameters.eta=0.5;
		out.println("eta=50%"+"\n\n");
		out.println("Recall\tRR\t\tPC\tPC\t\tPQ\t\tBKs");
		for(double i=0.0; i<=1.0; i+=0.1){
			DatasetDNF(i, method, d, dataset);
			out.print(p.format(i)+"\t");
			out.println(q.format(DNFBlocking.RR)+"\t"+DNFBlocking.PCount+"\t"+q.format(DNFBlocking.Pcomp)+"\t"+q.format(DNFBlocking.PQ)+"\t"+DNFBlocking.BKs);
		}
		out.println("\n\n");
		
		
		Parameters.eta=1.0;
		out.println("eta=100%"+"\n\n");
		out.println("Recall\tRR\t\tPC\tPC\t\tPQ\t\tBKs");
		for(double i=0.0; i<=1.0; i+=0.1){
			DatasetDNF(i, method, d, dataset);
			out.print(p.format(i)+"\t");
			out.println(q.format(DNFBlocking.RR)+"\t"+DNFBlocking.PCount+"\t"+q.format(DNFBlocking.Pcomp)+"\t"+q.format(DNFBlocking.PQ)+"\t"+DNFBlocking.BKs);
		}
		out.println("\n\n");
		
		out.close();
	}

	public static void UnSupervisedDisjunctFull(char method, char dataset,String file)throws IOException{
		Parameters.DNF=false;
		if(dataset=='r')
		{
			Parameters.ut=U_r_ut;
			Parameters.lt=U_r_lt;
			Parameters.dupDemanded=U_r_d;
			Parameters.nondupDemanded=U_r_nd;
		}
		else if(dataset=='c'){
			Parameters.ut=U_c_ut;
			Parameters.lt=U_c_lt;
			Parameters.dupDemanded=U_c_d;
			Parameters.nondupDemanded=U_c_nd;
		}
		else if(dataset=='s'){
			Parameters.ut=U_s_ut;
			Parameters.lt=U_s_lt;
			Parameters.dupDemanded=U_s_d;
			Parameters.nondupDemanded=U_s_nd;
		}
		PrintWriter out=new PrintWriter(new File(file));
		out.println(Parameters.ut+" "+Parameters.lt+" DD: "+Parameters.dupDemanded+" NDD: "+Parameters.nondupDemanded);
		DecimalFormat p=new DecimalFormat("0.00");
		DecimalFormat q=new DecimalFormat("0.000000");
		FeatureAnalysis d=null;
		d=UnsupervisedDatasetHelp(dataset);
		
		Parameters.eta=0.0;
		out.println("eta=1"+"\n\n");
		out.println("Recall\tRR\t\tPC\tPC\t\tPQ\t\tBKs");
		for(double i=0.0; i<=1.0; i+=0.1){
			Dataset(i, method, d, dataset);
			out.print(p.format(i)+"\t");
			out.println(q.format(DNFBlocking.RR)+"\t"+DNFBlocking.PCount+"\t"+q.format(DNFBlocking.Pcomp)+"\t"+q.format(DNFBlocking.PQ)+"\t"+DNFBlocking.BKs);
		}
		out.println("\n\n");
		
		
		Parameters.eta=0.1;
		out.println("eta=10%"+"\n\n");
		out.println("Recall\tRR\t\tPC\tPC\t\tPQ\t\tBKs");
		for(double i=0.0; i<=1.0; i+=0.1){
			Dataset(i, method, d, dataset);
			out.print(p.format(i)+"\t");
			out.println(q.format(DNFBlocking.RR)+"\t"+DNFBlocking.PCount+"\t"+q.format(DNFBlocking.Pcomp)+"\t"+q.format(DNFBlocking.PQ)+"\t"+DNFBlocking.BKs);
		}
		out.println("\n\n");
		
		
		Parameters.eta=0.5;
		out.println("eta=50%"+"\n\n");
		out.println("Recall\tRR\t\tPC\tPC\t\tPQ\t\tBKs");
		for(double i=0.0; i<=1.0; i+=0.1){
			Dataset(i, method, d, dataset);
			out.print(p.format(i)+"\t");
			out.println(q.format(DNFBlocking.RR)+"\t"+DNFBlocking.PCount+"\t"+q.format(DNFBlocking.Pcomp)+"\t"+q.format(DNFBlocking.PQ)+"\t"+DNFBlocking.BKs);
		}
		out.println("\n\n");
		
		
		Parameters.eta=1.0;
		out.println("eta=100%"+"\n\n");
		out.println("Recall\tRR\t\tPC\tPC\t\tPQ\t\tBKs");
		for(double i=0.0; i<=1.0; i+=0.1){
			Dataset(i, method, d, dataset);
			out.print(p.format(i)+"\t");
			out.println(q.format(DNFBlocking.RR)+"\t"+DNFBlocking.PCount+"\t"+q.format(DNFBlocking.Pcomp)+"\t"+q.format(DNFBlocking.PQ)+"\t"+DNFBlocking.BKs);
		}
		out.println("\n\n");
		
		out.close();
	}

	public static void UnSupervisedDNFFull(char method, char dataset,String file)throws IOException{
		Parameters.DNF=true;
		if(dataset=='r')
		{
			Parameters.ut=U_r_ut;
			Parameters.lt=U_r_lt;
			Parameters.dupDemanded=U_r_d;
			Parameters.nondupDemanded=U_r_nd;
		}
		else if(dataset=='c'){
			Parameters.ut=U_c_ut;
			Parameters.lt=U_c_lt;
			Parameters.dupDemanded=U_c_d;
			Parameters.nondupDemanded=U_c_nd;
		}
		else if(dataset=='s'){
			Parameters.ut=U_s_ut;
			Parameters.lt=U_s_lt;
			Parameters.dupDemanded=U_s_d;
			Parameters.nondupDemanded=U_s_nd;
		}
		PrintWriter out=new PrintWriter(new File(file));
		out.println(Parameters.ut+" "+Parameters.lt+" DD: "+Parameters.dupDemanded+" NDD: "+Parameters.nondupDemanded);
		DecimalFormat p=new DecimalFormat("0.00");
		DecimalFormat q=new DecimalFormat("0.000000");
		FeatureAnalysis d=null;
		d=UnsupervisedDatasetHelp(dataset);
		
		Parameters.eta=0.0;
		out.println("eta=1"+"\n\n");
		out.println("Recall\tRR\t\tPC\tPC\t\tPQ\t\tBKs");
		for(double i=0.0; i<=1.0; i+=0.1){
			DatasetDNF(i, method, d, dataset);
			out.print(p.format(i)+"\t");
			out.println(q.format(DNFBlocking.RR)+"\t"+DNFBlocking.PCount+"\t"+q.format(DNFBlocking.Pcomp)+"\t"+q.format(DNFBlocking.PQ)+"\t"+DNFBlocking.BKs);
		}
		out.println("\n\n");
		
		
		Parameters.eta=0.1;
		out.println("eta=10%"+"\n\n");
		out.println("Recall\tRR\t\tPC\tPC\t\tPQ\t\tBKs");
		for(double i=0.0; i<=1.0; i+=0.1){
			DatasetDNF(i, method, d, dataset);
			out.print(p.format(i)+"\t");
			out.println(q.format(DNFBlocking.RR)+"\t"+DNFBlocking.PCount+"\t"+q.format(DNFBlocking.Pcomp)+"\t"+q.format(DNFBlocking.PQ)+"\t"+DNFBlocking.BKs);
		}
		out.println("\n\n");
		
		
		Parameters.eta=0.5;
		out.println("eta=50%"+"\n\n");
		out.println("Recall\tRR\t\tPC\tPC\t\tPQ\t\tBKs");
		for(double i=0.0; i<=1.0; i+=0.1){
			DatasetDNF(i, method, d, dataset);
			out.print(p.format(i)+"\t");
			out.println(q.format(DNFBlocking.RR)+"\t"+DNFBlocking.PCount+"\t"+q.format(DNFBlocking.Pcomp)+"\t"+q.format(DNFBlocking.PQ)+"\t"+DNFBlocking.BKs);
		}
		out.println("\n\n");
		
		
		Parameters.eta=1.0;
		out.println("eta=100%"+"\n\n");
		out.println("Recall\tRR\t\tPC\tPC\t\tPQ\t\tBKs");
		for(double i=0.0; i<=1.0; i+=0.1){
			DatasetDNF(i, method, d, dataset);
			out.print(p.format(i)+"\t");
			out.println(q.format(DNFBlocking.RR)+"\t"+DNFBlocking.PCount+"\t"+q.format(DNFBlocking.Pcomp)+"\t"+q.format(DNFBlocking.PQ)+"\t"+DNFBlocking.BKs);
		}
		out.println("\n\n");
		
		out.close();
	}

	public static void setFeatureGenParams(double ut, double lt, int duplicates, int non_duplicates){
		Parameters.ut=ut;
		Parameters.lt=lt;
		Parameters.dupDemanded=duplicates;
		Parameters.nondupDemanded=non_duplicates;
	}
	
	public static void setSupervisedParams(int d1, int d2, int nd1, int nd2){
		Supervised.d1=d1;
		Supervised.d2=d2;
		Supervised.nd1=nd1;
		Supervised.nd2=nd2;
	}
	
	public static void setUnsupervisedParams(char dataset,double ut, double lt, int duplicates, int non_duplicates){
		if(dataset=='r'){
			Parameters.ut=ut;
			Parameters.lt=lt;
			Parameters.dupDemanded=duplicates;
			Parameters.nondupDemanded=non_duplicates;
		}
		else if(dataset=='c'){
			Parameters.ut=ut;
			Parameters.lt=lt;
			Parameters.dupDemanded=duplicates;
			Parameters.nondupDemanded=non_duplicates;
		}
		else if(dataset=='s'){
			Parameters.ut=ut;
			Parameters.lt=lt;
			Parameters.dupDemanded=duplicates;
			Parameters.nondupDemanded=non_duplicates;
		}
	}
	
	
	private static FeatureAnalysis SupervisedDatasetHelp(char dataset)throws IOException{
		FeatureGen b=null;
		if(dataset=='r'){
			Supervised a=new Supervised(rPath+"GoldStandard.csv",'r');
			b=new FeatureGen(a,num_feats*r_att,r_att,rPath+"restaurantRecords.csv");
		}
			
		else if(dataset=='c'){
			Supervised a=new Supervised(cPath+"GoldStandard.csv",'c');
			b=new FeatureGen(a,num_feats*c_att,c_att,cPath+"coraRecords.csv");
	
		}
			
		else if(dataset=='s'){
			Supervised a=new Supervised(sPath+"GoldStandard.csv",'s');
			b=new FeatureGen(a,num_feats*s_att,s_att,sPath+"censusRecords.csv");
	
		}
			
		b.setUpFeatures();
		Fisher c=new Fisher(b.getNum_feats(),b.getDupFeatures(),b.getNondupFeatures());
		System.out.println("computing statistics");
		c.computeStatistics();
		FeatureAnalysis d=new FeatureAnalysis(c);
		return d;
	}
	
	private static FeatureAnalysis UnsupervisedDatasetHelp(char dataset) throws IOException{
		FeatureGen b=null;
		if(dataset=='r'){
			Unsupervised a=new Unsupervised(rPath+"restaurantRecords.csv");
			b=new FeatureGen(a,r_att*num_feats, 'r');
		}
			
		else if(dataset=='c'){
			Unsupervised a=new Unsupervised(cPath+"coraRecords.csv");
			b=new FeatureGen(a,c_att*num_feats, 'c');
		}
			
		else if(dataset=='s'){
			Unsupervised a=new Unsupervised(sPath+"censusRecords.csv");
			b=new FeatureGen(a,s_att*num_feats, 's');
		}
			
		
		
		b.setUpFeatures();
		Fisher c=new Fisher(b.getNum_feats(),b.getDupFeatures(),b.getNondupFeatures());
		System.out.println("computing statistics "+b.getNum_feats()+" "+b.getDupFeatures().get(0).size());
		c.computeStatistics();
		FeatureAnalysis d=new FeatureAnalysis(c);
		//d.printBestFeatures();
		return d;
	}
	
	private static void Dataset(double recall,char method,FeatureAnalysis d, char dataset)throws IOException{
		System.out.println("recall : "+recall);
		LearnDisjunct e=null;
		if(dataset=='r')
			e=new LearnDisjunct(d,1,2,r_att,recall);
		else if(dataset=='c')
			e=new LearnDisjunct(d,1,2,c_att,recall);
		else if(dataset=='s')
			e=new LearnDisjunct(d,1,2,s_att,recall);
		
		ArrayList<String> codes=null;
		if(method=='f'){
		e.populateDisjunction_Features();
		codes= e.codes();
		
		}
		else if(method=='b'){
			e.populateDisjunction_Bilenko();
			codes=e.codes();
		
		}
		
		
		if(dataset=='r'){
			DNFBlocking a=new DNFBlocking(rPath+"restaurantRecords.csv");
			if(method=='f')
				a.process(codes,rPath+"GoldStandard.csv",'r');
			else
				a.process(codes,rPath+"GoldStandard.csv",'r');
		}
		
		else if(dataset=='c'){
			DNFBlocking a=new DNFBlocking(cPath+"coraRecords.csv");
			if(method=='f')
				a.process(codes,cPath+"GoldStandard.csv",'c');
			else
				a.process(codes,cPath+"GoldStandard.csv",'c');
		}
		else if(dataset=='s'){
			DNFBlocking a=new DNFBlocking(sPath+"censusRecords.csv");
			if(method=='f')
			a.process(codes,sPath+"GoldStandard.csv",'s');
			else
				a.process(codes,sPath+"GoldStandard.csv",'s');
		}
		
		
	}
	
	private static void DatasetDNF( double recall,char method,FeatureAnalysis d, char dataset)throws IOException{
		
		
		System.out.println("recall : "+recall);
		LearnDisjunct e=null;
		if(dataset=='r')
			e=new LearnDisjunct(d,1,2,r_att,recall);
		else if(dataset=='c')
			e=new LearnDisjunct(d,1,2,c_att,recall);
		else if(dataset=='s')
			e=new LearnDisjunct(d,1,2,s_att,recall);
		ArrayList<String> codesDNF=null;
		if(method=='f'){
			e.populateDNF_Features(conjuncts);
			
		
		}
		else{
			e.populateDNF_Bilenko(conjuncts);
			
		
		}
		codesDNF=e.codesDNF();
		
		
		if(dataset=='r'){
			DNFBlocking a=new DNFBlocking(rPath+"restaurantRecords.csv");
			if(method=='f')
				a.processDNF(codesDNF,rPath+"GoldStandard.csv",'r');
			else
				a.processDNF(codesDNF,rPath+"GoldStandard.csv",'r');
		}
		else if(dataset=='s'){
			DNFBlocking a=new DNFBlocking(sPath+"censusRecords.csv");
			if(method=='f')
			a.processDNF(codesDNF,sPath+"GoldStandard.csv",'s');
			else
				a.processDNF(codesDNF,sPath+"GoldStandard.csv",'s');
		
		}
		else if(dataset=='c'){
			DNFBlocking a=new DNFBlocking(cPath+"coraRecords.csv");
			if(method=='f')
				a.processDNF(codesDNF,cPath+"GoldStandard.csv",'c');
			else
				a.processDNF(codesDNF,cPath+"GoldStandard.csv",'c');
		}
		
		
	}

	

}
