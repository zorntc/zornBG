import java.util.*;

public class Design {

	static int[] workloadCnt = {};
	
	static String[] actionName = {};
	static boolean[] readOnly = {};

	static String[] column = {};
	static String[] table = {};
	static String[] queryCommand = {};

	private LinkedList<Table> partitionList = new LinkedList<Table>();
	private LinkedList<Table> replicationList = new LinkedList<Table>();
	private LinkedList<Procedure> routAtrrList = new LinkedList<Procedure>(); 

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
			if(!readOnly[i]){
				ta.replication = false;
				ta.modifiedCol.add(column[i]);
			}
		}
	}

	private void replication(){
		ListIterator<Table> ite = partitionList.listIterator(0);
		Table t;
		while(ite.hasNext()){
			t = ite.next();
			if(t.replication)
				replicationList.add(t);
		}

		ite = replicationList.listIterator(0);
		while(ite.hasNext()){
			t = ite.next();
			partitionList.remove(t);
		}
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
		}		
	}

	private void printlog(){
		System.out.println("== PARTITION & SECONDARY INDEX ==");
		for(Table tl: partitionList){
			System.out.printf("partition %s by %s, second index: %s\n", tl.name, tl.getPartAttr(), tl.getSecIndex());
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
		}
		
		
		/* DEBUG */
		for(i=0;i<readOnly.length;i++)			
	           System.out.println(column[i]+'\t'+table[i]+'\t'+queryCommand[i]+'\t'+readOnly[i]);
	   
		
		if(isArgLengthEqual() != null){
			System.err.println("arrays length is inconsistent: " + isArgLengthEqual());
			System.exit(1);
		}
	}
	
	public static void main(String[] args){
		
		passArgumets();
		
		Design best = new Design();
		best.init();
		best.printlog();
	}
}


