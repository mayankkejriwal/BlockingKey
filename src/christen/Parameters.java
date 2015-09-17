package christen;

public class Parameters {
	public static int c=20;
	public static double ut=0.05;
	public static double lt=0.000001;
	//public static String splitstring="\\[|\\]|\\(|\\)|: | |,|:|/|-|_"; //older version
	public static String splitstring="://|\\[|\\]|\\(|\\)|: |\t| |,|:|/|-|_|\\. |\\.|\\&|\"|#|;|; ";
	public static int conjuncts=2;//k in paper
	public static int num_feats=28; 
	public static int dupDemanded=2000; //max N size
	public static int nondupDemanded=2000; //max D size
	public static double recall=0.99; //epsilon: more is better, the less noisy the sample
	public static double eta=0.3; //less the better, less noisy the sample
	public static boolean DNF=false;
	public static String SVM="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/SVM.model";
	public static int maxpairs=Integer.MAX_VALUE; //hetero parameter; also used in canopy clustering:
									//WHILE blocking, not during discovery
	public static int maxtokentuples=Integer.MAX_VALUE; //maxBucketPairs in paper
	public static int maxmapperoutput=30; //maxBucketTuples in paper
	public static String[] forbiddenwords={"null"}; //interpreted as case insensitive
}
