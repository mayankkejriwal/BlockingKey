package Tests;

import java.util.*;

import FeatureSelection.Fisher;

public class FisherTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int[][] class1={{1,0,0,0},{1,1,1,0},{0,1,0,0},{0,0,0,1}};
		int[][] class2={{1,0,1,0},{1,1,0,0},{1,1,0,0},{1,0,0,1}};
		int[][] class3={{1,0,0,1},{1,0,1,0},{0,1,1,0},{0,0,1,1}};
		/*HashSet<ArrayList<Integer>> s=convertToHashSet(class2);
		Iterator<ArrayList<Integer>> p=s.iterator();
		while(p.hasNext())
			System.out.println(p.next());*/
		Fisher t=new Fisher(4,convertToArrayList(class1),convertToArrayList(class2),convertToArrayList(class3));
		t.computeStatistics();
		printFeatures(t);
		printStatistics(t);
	}
	
	public static void printStatistics(Fisher t){
		System.out.println("Features: "+t.get_num_features());
		System.out.println("Total Samples: "+t.get_total_samples());
		for(int i=0; i<t.get_classes().size(); i++){
			System.out.println("Class: "+t.get_classes().get(i));
			System.out.println("Mean: ");
			printArrayList(t.get_mean().get(i));
			System.out.println("Variance: ");
			printArrayList(t.get_variance().get(i));
			
		}
		
		System.out.println("Overall Mean: ");
		printArrayList(t.get_overall_mean());
		System.out.println("Overall Variance: ");
		printArrayList(t.get_overall_variance());
		System.out.println("Fisher Scores: ");
		printArrayList(t.get_scores());
		
		
	}
	
	public static ArrayList<ArrayList<Integer>> convertToArrayList(int[][] class1){
		ArrayList<ArrayList<Integer>> result=new ArrayList<ArrayList<Integer>>();
		for(int i=0; i<class1.length; i++){
			ArrayList<Integer> m=new ArrayList<Integer>();
			convertToArrayList(m,class1[i]);
			result.add(m);
		}
			
		return result;
	}
	public static void convertToArrayList(ArrayList<Integer> result,int[] array){
		
		for(int i=0; i<array.length; i++)
			result.add(array[i]);
		
	}
	public static void printArrayList(ArrayList<Double> a){
		
			System.out.print(a+" ");
		System.out.println();
	}
	public static void printFeatures(Fisher t){
		for(int i:t.features.keySet()){
			System.out.println("Class: "+i);
			for(int j=0;j<t.features.get(i).size();j++)
				System.out.println(t.features.get(i).get(j));
		}
			
	}

}
