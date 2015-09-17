package Tests;

import christen.BK;

public class TestBK {

	static String splitstring="\\[|\\]|\\(|\\)| \"|: | |\"|,|:|/|-|_";
	/**
	 * @param args
	 */
	
	public static void main(String...args){
		String[] a=new String(" \"212/ 319-1660\"").split(splitstring);
		for(int i=0; i<a.length; i++)
			System.out.println(a[i]);
		testCommonTokenNGram();
	}
	
	public static void testExactMatch(){
		String[] arr1={"sadas","cat","asfdf","mat","dfsdf","sdfsdf"};
		String[] arr2={"ewrewr","dsfsdf","cat","Mat","sdfdsf","dsfsdf"};
		int[] exp={0,0,0,1,0,0};
		int[] wt=BK.ExactMatch(arr1, arr2);
		if(checkArraysEqual(wt,exp))
			System.out.println("Success");
		else
			System.out.println("Failure");
	}
	
	public static void testCommonToken(){
		String[] arr1={"sadas","cat","asfdf","mat","dfsdf","dsdf sdf sdf"};
		String[] arr2={"ewrewr","dsfsdf","cat","mat,gfgf:df","sdfdsf","dsf,sdf"};
		int[] exp={0,0,0,1,0,1};
		int[] wt=BK.CommonToken(arr1, arr2);
		if(checkArraysEqual(wt,exp))
			System.out.println("Success");
		else
			System.out.println("Failure");
	}
	
	public static void testCommonInteger(){
		String[] arr1={"sadas 1,5,3","cat 3","asfdf 4","mat 6","dfsdf","dsdf 6 sdf sdf"};
		String[] arr2={"ewrewr 1:3:4","dsfsdf 2","cat 5 4","mat,gfgf:df: 786","sdfdsf","dsf,sdf (6)"};
		int[] exp={1,0,1,0,0,1};
		int[] wt=BK.CommonInteger(arr1, arr2);
		printWeight(wt);
		if(checkArraysEqual(wt,exp))
			System.out.println("Success");
		else
			System.out.println("Failure");
	}
	
	public static void testCommonOrOffByOneInteger(){
		String[] arr1={"sadas 1,5,3","cat 3","asfdf 4","mat786 789","dfsdf","dsdf 6 sdf sdf"};
		String[] arr2={"ewrewr 1:3:4","dsfsdf 2","cat 5","mat,gfgf:df: 786","sdfdsf","dsf,sdf (7)"};
		int[] exp={1,1,1,0,0,1};
		int[] wt=BK.CommonOrOffByOneInteger(arr1, arr2);
		printWeight(wt);
		if(checkArraysEqual(wt,exp))
			System.out.println("Success");
		else
			System.out.println("Failure");
	}
	
	public static void testCommonNFirst(){
		String[] arr1={"sadas 1,5,3","cat 3","asfdf 4","mat786 789","df sd f","d sdf 6 sdf sdf"};
		String[] arr2={"ewrewr 1:3:4","dsfsdf 2","cat 5","met,gfgf:df: 786","sdfdsf","dsf,sdf (7)"};
		int[] exp={0,0,0,1,1,1};
		int[] wt=BK.CommonNFirst(arr1, arr2,2);
		printWeight(wt);
		if(checkArraysEqual(wt,exp))
			System.out.println("Success");
		else
			System.out.println("Failure");
	}
	
	public static void testCommonTokenNGram(){
		String[] arr1={"310/246-1501","cat 3","asfdf 4","mat 786 789","df sd f","d sdf 6 sdf sdf"};
		String[] arr2={"310-246-1501","dsfcasdf 2 3 cat 3","cat 5","met,gfgf:df: 786","sdfdsf","dsf,sdf (6)"};
		int[] exp={1,1,0,0,0,0};
		int[] wt=BK.CommonTokenNGram(arr1, arr2,3);
		printWeight(wt);
		if(checkArraysEqual(wt,exp))
			System.out.println("Success");
		else
			System.out.println("Failure");
	}
	
	private static void printWeight(int[] wt){
		for(int i=0; i<wt.length; i++)
			System.out.print(wt[i]+" ");
	}
	private static boolean checkArraysEqual(int[] m, int[]n){
		if(m.length!=n.length)
			return false;
		for(int i=0; i<m.length; i++)
			if(m[i]!=n[i])
				return false;
		return true;
	}
	
}
