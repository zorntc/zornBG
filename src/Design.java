import java.util.*;

public class Design {

	static boolean[] readOnly = {};
	static int[] workloadCnt = {};
	static String[] actionName = {};
	static String[] column = {};
	static String[] queryCommand = {};
	static String[] table = {};
	
	LinkedList<Table> partitionList = new LinkedList<Table>();
	LinkedList<Table> replicationList = new LinkedList<Table>();
	LinkedList<Table> tableList = new LinkedList<Table>();
	LinkedList<Procedure> routAtrrList = new LinkedList<Procedure>(); 
	private int num_tables;
	
	public Design(Design d){
		this.partitionList = new LinkedList<Table>(d.partitionList);
		this.replicationList = new LinkedList<Table>(d.replicationList);
		this.tableList = new LinkedList<Table>(d.tableList);
		this.routAtrrList = new LinkedList<Procedure>(d.routAtrrList); 
		this.num_tables = d.num_tables;
	}
	
	public Design(){
		super();
	}
	
	int getNumTables(){
		return num_tables;
	}
	
	LinkedList<Table> getPartitionList(){
		return partitionList;
	}

	public static String isArgLengthEqual(){
		return (workloadCnt.length != actionName.length)? "workloadCnt or actionName" : 
			(workloadCnt.length != readOnly.length)? "readOnly" :
			(workloadCnt.length != column.length)? "column" :
			(workloadCnt.length != table.length)? "table" : null;
	}

	private void partition(){
		int i;
		Table ta = null;
		for(i = 0; i < column.length; i++){
			if(workloadCnt[i] <= 0) continue;
			ta = null;
			for(Table t : partitionList){
				if(!t.name.equalsIgnoreCase(table[i]))
					continue;
				ta = t;
				break;
			}
			if(ta == null){						 
				partitionList.add(new Table(table[i]));
				ta = partitionList.getLast();
			}
			ta.inc(column[i], workloadCnt[i]);
			ta.childrenProcedureName.add(actionName[i].toLowerCase());
			if(!readOnly[i]){
				ta.replication = false;
				ta.modifiedCol.add(column[i]);
			}
		}
		
		// sort Table
		Table[] partitionArray = {};
		partitionArray = partitionList.toArray(partitionArray);
		Arrays.sort(partitionArray);
		partitionList = new LinkedList<Table>();
		for(Table t : partitionArray)
			partitionList.add(t);
	}

	private void replication(){
		Iterator<Table> ite;
		Table t;		
		boolean found;
		
		for(SchemaExtractor se : HorticultureFinalProject.schemaExtractor){
			found = false;
			for(Table ta : partitionList){
				if(se.getTableName().equalsIgnoreCase(ta.name)){
					found = true;
					break;							
				}
			}
			
			if(!found)
				replicationList.add(new Table(se.getTableName()));
		}
		
		ite = partitionList.descendingIterator();
		while(ite.hasNext()){
			t = ite.next();
			if(!t.replication)
				continue;
			replicationList.addFirst(t);
			ite.remove();
		}
		
		num_tables = partitionList.size() + replicationList.size();
		for(Table ta: partitionList)
			tableList.add(ta);
		for(Table ta: replicationList)
			tableList.add(ta);
	}

	private void routingAttr(){
		int i;
		Procedure pr = null;
		for(i = 0; i < column.length; i++){
			pr = null;
			for(Procedure p : routAtrrList){
				if(!p.name.equals(actionName[i]))
					continue;
				pr = p;
				break;
			}
			if(pr == null){						 
				routAtrrList.add(new Procedure(actionName[i], this));
				pr = routAtrrList.getLast();
			}
			pr.addAttr(column[i], table[i]);
			pr.setFreq(workloadCnt[i]);
		}
		
		// sort routingAttr
		Procedure[] routingArray = {};
		routingArray = routAtrrList.toArray(routingArray);
		Arrays.sort(routingArray);
		routAtrrList = new LinkedList<Procedure>();
		for(Procedure t : routingArray)
			routAtrrList.add(t);
		
		// routAtrrList set, set childrenProcedure for every table
		for(Table ta: partitionList){
			for(String s: ta.childrenProcedureName){
				for(i = routAtrrList.size() - 1; i >= 0; i--){
					if(s.equalsIgnoreCase(routAtrrList.get(i).name))
						ta.childrenProcedure.add(i);
				}
			}
		}
		for(Table ta: replicationList){
			for(String s: ta.childrenProcedureName){
				for(i = routAtrrList.size() - 1; i >= 0; i--){
					if(s.equalsIgnoreCase(routAtrrList.get(i).name))
						ta.childrenProcedure.add(i);
				}
			}
		}
	}

	private void printlog(String s){
		System.out.println("  = = = " + s + " = = =");
		printlog();
	}
	
	private void printlog(){
		System.out.println("== PARTITION & SECONDARY INDEX ==");
		for(Table tl: partitionList){
			System.out.printf("partition %s by %s, secondary index: %s\n", tl.name, tl.getPartAttr(), tl.getSecIndex());
		}
		System.out.println("== REPLICATION ==");
		for(Table tl: replicationList){
			System.out.printf("replicate %s\n", tl);
		}
		System.out.println("== ROUTING ATTRIBUTE ==");
		for(Procedure p: routAtrrList){
			System.out.printf("Routing attribute of %s = %s\n", p.name, p.getRoutAtrr());
		}
	}
	
	private void init(){
		partition();	// is not finished without calling replication()
		replication();
		routingAttr();		
	}

	
	static void passArgumets(){
		String[] nil = null;
		
		// passing Arpit xml file 
		HorticultureFinalProject.main(nil);
		
		// Get entries from HorticultureFinalProject
		column = HorticultureFinalProject.getAttributeNames(column);
		table = HorticultureFinalProject.getTableNames(table);
		queryCommand = HorticultureFinalProject.getQueryNames(queryCommand);
		readOnly = HorticultureFinalProject.getReadOnly(readOnly);
		actionName = HorticultureFinalProject.getActionNames(actionName);
		workloadCnt = new int[actionName.length];
				
		int i;
		String s;
		
		for(i = actionName.length - 1; i >= 0; i--){
			for(WorkloadExtractor wle : HorticultureFinalProject.workloadExtractor){
				s = wle.getAction();
			if(actionName[i].equalsIgnoreCase(s)){
				workloadCnt[i] = Integer.parseInt(wle.getFrequency());
				break;
				}
			}
			column[i] = column[i].trim(); 
		}
		
		
		/* DEBUG */
		for(i=0;i<readOnly.length;i++)			
	           System.out.println(actionName[i] +'\t'+ column[i]+'\t'+table[i]+'\t'+queryCommand[i]+'\t'+readOnly[i]);
	   
		
		if(isArgLengthEqual() != null){
			System.err.println("arrays length is inconsistent: " + isArgLengthEqual());
			System.exit(1);
		}
	}
	
	public static void main(String[] args){
		
		passArgumets();
		
		Design best = new Design();
		best.init();
		best.printlog("Initial Design");
		
		LNS.relaxThenSearch(best).printlog("Final Design");
	}
}


