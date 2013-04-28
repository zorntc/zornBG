import java.util.*;

public class LNS {

	// environmental parameters
	static final int LOCAL_SEARCH_ROUND = 1000;
	static final int noImproveRndMax = 100;		// max round number allowed for local procedure search without improving 
	static final double relaxation_factor_min = 0.25;
	static final double relaxation_factor_max = 0.5;
	
	static int noImproveRnd = 0;		// global conuter for noImproveRnd;
	
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
	
	static int relaxRound = 0;
	static int relaxTableQuantity = 1;
	static boolean[][] flip;	// corresponding to principle table's name
	static boolean[] flipBase;
	static int combination = 1;

	public static boolean relaxTable(){
		int i;
		for(i = 0; i < partitionTableSize; i++)
			combination *= 2;
		
		flipBase = new boolean[num_tables];
		flip = new boolean[combination - 1][partitionTableSize];
		for(i = 1; i <= partitionTableSize; i++)
			forward(i, 0);
		return true;
	}
	
	static void forward(int digit, int start){
		if(start >= partitionTableSize || digit <= 0)
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
		for(i = 0; i < partitionTableSize; i++)
			flip[inscribeOrder][i] = flipBase[i];
		inscribeOrder++;
	}
	
	static boolean addRelaxProc(Table t){
		return relaxedProcedureIndex.addAll(t.childrenProcedure);
	}
	
	public static void setRelaxTable(int index){
		relaxedProcedureIndex = new TreeSet<Integer>();  
		current = new Design(principle);
		current.partitionList.clear();
		
		LinkedList<Table> dupPart = new LinkedList<Table>(principle.partitionList); 
		Table tmp;
		int i;

		for(i = 0; i < partitionTableSize; i++){
			tmp = dupPart.get(i);
			if(flip[index][i]){
				current.replicationList.add(tmp);
				addRelaxProc(tmp);
			}
			else
				current.partitionList.add(tmp);
		}

		// clearRouteAtrr()
		for(Integer ind: relaxedProcedureIndex)
			current.routAtrrList.get(ind).clearRouteAtrr();
		
		return;
	}
	
	static boolean compareCost(Design de){
		double newCost = estimateCost(de);
		if(newCost >= bestCost)
			return false;
		setBest(de, newCost);
		return true;
	}
	
	public static boolean searchProcedure(TreeSet<Integer> routingUnknown){
		if(noImproveRnd++ > LOCAL_SEARCH_ROUND)
			return false;
		
		if(routingUnknown.size() <= 0)
			return compareCost(current);
		
		int index = routingUnknown.pollFirst();
		boolean ret = false;
		Procedure pr = current.routAtrrList.get(index);
		for(String s : pr.attrCdt){
			pr.routAtrr = s;
			if(estimateCost(current) >= bestCost)
				continue;
			if(searchProcedure(routingUnknown))
				ret = true;
		}
		return ret;
	}
	
	public static boolean fixRelaxTable(int tableIndex){	
		boolean ret = false;
		if(tableIndex >= partitionTableSize)
			return searchProcedure(relaxedProcedureIndex);
		
		// FIXME possible to fail when no attribute Candidates
		if(!flipBase[tableIndex])				
			return fixRelaxTable(tableIndex + 1);
		if(current.tableList.get(tableIndex).replication
				|| (current.tableList.get(tableIndex).attrCdt.size() == 0))
			System.err.println("Something Wrong with fixRelaxTable()");
		
		Table tmp = current.tableList.get(tableIndex);
		// use all attrCdt to set routing attribute
		for(String attrC: tmp.attrCdt){	
			current.tableList.get(tableIndex).fixRelaxPartitionAttr(attrC);
			if(fixRelaxTable(tableIndex + 1))
				ret = true;
		}
		
		// replicate originally partitioned table
		current.partitionList.remove(tmp);
		current.replicationList.addFirst(tmp);
		if(fixRelaxTable(tableIndex + 1))
			ret = true;
		
		return ret;
	}
	
	static boolean search(int round){
		int i;
		
		flipBase = new boolean[partitionTableSize];
		for(i = 0; i < partitionTableSize; i++)
			flipBase[i] = flip[round][i];
		
		return fixRelaxTable(0);
	}
	
	public static Design relaxThenSearch(Design d) {
		int i;
		
		setPrinciple(d);
		setBest(d, estimateCost(d));
		
		relaxTable();
		for(i = 0; i < combination - 1; i++){
			setRelaxTable(i);
			
			/*
			noImproveRnd = 0;
			while(noImproveRnd < noImproveRndMax){
				if(search(i))
					noImproveRnd = 0;
				else
					noImproveRnd++;
			}
			*/
			search(i);
		}
		return best;
	}
	
	static void setPrinciple(Design d){
		principle = new Design(d);
		num_tables = d.getNumTables();
		partitionTableSize = d.partitionList.size();
	}
	
	static void setBest(Design de, double dou){
		best = new Design(de);
		bestCost = dou;
		noImproveRnd = 0;
	}
	
	static double cost = 0;
	public static double estimateCost(Design d){
		return cost++;
	}
}
