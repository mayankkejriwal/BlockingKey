package christen;
import java.text.DecimalFormat;
import java.util.*;
import java.io.*;

public class DataAnalysis {
	
	public static void runAnalysis(String dataFile)throws IOException{
		ArrayList<Double[][]> m=computeFileAverage(dataFile);
		printDoubleList(findBest(m));
		double[] q=averageAll(m);
		printDouble(q);
		//printDoubleList(averageIndividual(m));
	}
	
	private static ArrayList<Double[][]> computeFileAverage(String file)throws IOException{
		Scanner in=new Scanner(new File(file));
		int count=0;
		while(count<=4){
			if(in.hasNextLine()) in.nextLine();
			count++;
			
		}
		count=0;
		ArrayList<String> p1=new ArrayList<String>();
		
		while(count<11){
			if(in.hasNextLine()) p1.add(in.nextLine());
			count++;
		}
		count=0;
		while(count<=6){
			if(in.hasNextLine()) in.nextLine();
			count++;
			
		}
		count=0;
		ArrayList<String> p2=new ArrayList<String>();
		
		while(count<11){
			if(in.hasNextLine()) p2.add(in.nextLine());
			count++;
		}
		
		count=0;
		while(count<=6){
			if(in.hasNextLine()) in.nextLine();
			count++;
			
		}
		count=0;
		ArrayList<String> p3=new ArrayList<String>();
		
		while(count<11){
			if(in.hasNextLine()) p3.add(in.nextLine());
			count++;
		}
		
		count=0;
		while(count<=6){
			if(in.hasNextLine()) in.nextLine();
			count++;
			
		}
		count=0;
		ArrayList<String> p4=new ArrayList<String>();
		
		while(count<11){
			if(in.hasNextLine()) p4.add(in.nextLine());
			count++;
		}
		
		in.close();
	
		ArrayList<Double[][]> res=new ArrayList<Double[][]>();
		
		res.add(convertArrayList(p1));
		res.add(convertArrayList(p2));
		res.add(convertArrayList(p3));
		res.add(convertArrayList(p4));
		
		return res;
	}
	
	private static Double[][] convertArrayList(ArrayList<String> m){
		Double[][] res=new Double[11][7];
		
		for(int i=0; i<m.size(); i++){
			String[] q=m.get(i).split("\\s");
			for(int j=0; j<=5; j++)
				if(j!=4)
				res[i][j]=Double.valueOf(q[j]);
				else
					res[i][j]=0.0;
			res[i][6]=2*res[i][1]*res[i][3]/(res[i][1]+res[i][3]);
		}
		
		return res;
	}
	
	
	private static double[] averageAll(ArrayList<Double[][]> q){
		
		double[] tmp=new double[7];
		for(int i=0; i<q.size(); i++)
			for(int k=0; k<11; k++)
			for(int j=0; j<7; j++)
				tmp[j]+=(q.get(i)[k][j]/(q.size()*11));
		return tmp;
	}
	
	
	//Column 4 (counting from 0) now contains best_eta
	private static ArrayList<double[]> findBest(ArrayList<Double[][]> m){
		ArrayList<double[]> res=new ArrayList<double[]>();
		double best=0.0;
		double best_eta=-1;
		double[] d=new double[7];
		for(int i=0; i<m.size(); i++)
			
			for(int k=0; k<11; k++)
			
				if((double)m.get(i)[k][6]>=best){
					best=(double) m.get(i)[k][6];
					d=copyArray(m.get(i)[k]);
					if(i==0)
						best_eta=0.0;
					else if(i==1)
						best_eta=0.1;
					else if(i==2)
						best_eta=0.5;
					else
						best_eta=1.0;
				}
				d[4]=best_eta;
			res.add(d);
			return res;
		}
	
	private static double[] copyArray(Double[] d){
			double[] res=new double[7];
			for(int i=0; i<7; i++)
				res[i]=(double) d[i];
			return res;
		}
	
	
	
	private static void printDoubleList(ArrayList<double[]> d){
		for(int i=0; i<d.size(); i++)
			printDouble(d.get(i));
	}
	
	private static void printDouble(double[] d){
		System.out.println();
		//DecimalFormat p=new DecimalFormat("0.00");
		DecimalFormat q=new DecimalFormat("0.000000");
		for(int i=0; i<d.length; i++)
			System.out.print(q.format(d[i])+" ");
	}

}
