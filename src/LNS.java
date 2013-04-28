import java.util.*;

public class LNS {

	// environmental parameters
	static final int LOCAL_SEARCH_ROUND = 1000;
	static final int noImproveRndMax = 100;		// max round number allowed for local procedure search without improving 
	static final double relaxation_factor_min = 0.25;
	static final double relaxation_factor_max = 0.5;
	
	// currently best design
	private static Design best;			
	private static double bestCost;
	
	// current Design
	private static Design current;
	private static TreeSet<Integer> relaxedProcedureIndex = new TreeSet<Integer>();
	
	// principles Design
	static Design principle;	// contains parsing info;
	static int num_tables;
	static int partitionTableSize;
	static String[] principleTableName;
	
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
		LinkedList<Table> dupPart = new LinkedList<Table>(principle.partitionList); 
		LinkedList<Table> dupRep = new LinkedList<Table>(principle.replicationList); 
		Table tmp;
		int i;

		for(i = 0; i < partitionTableSize; i++){
			if(flip[index][i]){
				tmp = dupPart.get(i);
				tmp.relax();
				current.replicationList.add(tmp);
				addRelaxProc(tmp);
			}
			else
				current.partitionList.add(dupPart.get(i));
		}
		for(i = partitionTableSize; i < num_tables; i++){
			if(flip[index][i]){
				tmp = dupRep.get(i - partitionTableSize);
				tmp.relax();
				current.partitionList.add(tmp);
				addRelaxProc(tmp);
			}
			else
				current.replicationList.add(dupRep.get(i - partitionTableSize));
		}
		
		// clearRouteAtrr()
		for(Integer ind: relaxedProcedureIndex)
			current.routAtrrList.get(ind).clearRouteAtrr();
		
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
		principleTableName = new String[num_tables];
		
		int i;
		for(i = 0; i < partitionTableSize; i++)
			principleTableName[i] = d.partitionList.get(i).name;
		for(i = partitionTableSize; i < num_tables; i++)
			principleTableName[i] = d.replicationList.get(i - partitionTableSize).name;
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
