import java.util.*;

public class LNS {

	static final int LOCAL_SEARCH_ROUND = 1000;
	static final int noImproveRndMax = 100;		// max round number allowed for local procedure search without improving 
	static final double relaxation_factor_min = 0.25;
	static final double relaxation_factor_max = 0.5;
	
	

	private static double bestCost;
	private static Design best;			// currently best design
	
	private static Design current;
	private static TreeSet<Integer> relaxedProcedureIndex = new TreeSet<Integer>();
	
	static Design principle;	// contains parsing info;
	static int num_tables;
	static int partitionTableSize;
	
	static int relaxRound = 0;
	static int relaxTableQuantity = 1;
	static boolean[][] flip;	// corresponding to principle table's name
	static boolean[] flipBase;
	static int combination = 1;

	public static boolean relaxTable(){
		int i;
		for(i = 0; i < num_tables; i++)
			combination *= 2;
		
		flipBase = new boolean[num_tables];
		flip = new boolean[combination - 1][num_tables];
		for(i = 1; i <= num_tables; i++)
			forward(i, 0);
		return true;
	}
	
	static void forward(int digit, int start){
		if(start >= num_tables || digit <= 0)
			return;

		flipBase[start] = true;
		if(digit == 1)
			inscribe();
		forward(digit - 1, start + 1);
		flipBase[start] = false;
		forward(digit, start + 1);
	}
	
	static int inscribeOrder = 0;
	static void inscribe(){
		int i;
		for(i = 0; i < num_tables; i++)
			flip[inscribeOrder][i] = flipBase[i];
		inscribeOrder++;
	}
	
	static boolean addRelaxProc(Table t){
		return relaxedProcedureIndex.addAll(t.childrenProcedure);
	}
	
	public static void setRelaxTable(int index){
		relaxedProcedureIndex = new TreeSet<Integer>();  
		current = new Design();
		current.routAtrrList = new LinkedList<Procedure>(principle.routAtrrList);
		
		Table tmp;
		int i;
		for(i = 0; i < partitionTableSize; i++){
			if(flip[index][i]){
				tmp = principle.partitionList.get(i);
				current.replicationList.add(tmp);
				addRelaxProc(tmp);
			}
			else
				current.partitionList.add(principle.partitionList.get(i));
		}
		for(i = partitionTableSize; i < num_tables; i++){
			if(flip[index][i]){
				tmp = principle.replicationList.get(i - partitionTableSize);
				current.partitionList.add(tmp);
				addRelaxProc(tmp);
			}
			else
				current.replicationList.add(principle.replicationList.get(i - partitionTableSize));
		}
		
		// clearRouteAtrr()
		for(i = current.routAtrrList.size() - 1; i >= 0; i--)
			current.routAtrrList.get(i).clearRouteAtrr();
		
		return;
	}
	
	static boolean procSearch(){	// TODO
		return false;
	}
	
	public static void search(Design d) {
		int i;
		int noImproveRnd;
		
		setPrinciple(d);
		setBest(d, estimateCost(d));
		
		relaxTable();
		for(i = 0; i < combination - 1; i++){
			setRelaxTable(i);
			
			noImproveRnd = 0;
			while(noImproveRnd < noImproveRndMax){
				if(procSearch())
					noImproveRnd = 0;
				else
					noImproveRnd++;
			}
		}
	}
	
	static void setPrinciple(Design d){
		principle = new Design(d);
		num_tables = d.getNumTables();
		partitionTableSize = d.partitionList.size();
	}
	
	static void setBest(Design de, double dou){
		best = new Design(de);
		bestCost = dou;
	}
	
	static double cost = 0;
	public static double estimateCost(Design d){
		return cost++;
	}
}
