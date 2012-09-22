package Transaction;

import java.util.*;
import java.util.List;

public class Transaction {
	public boolean ReadOnly;
	public int startTime;
	public int status;
	public int index;
	public List<Message> operations;
	public List<int[]> readvalues;
	
	public Transaction(int index,boolean readonly,int starttime)
	{
		this.index = index;
		this.ReadOnly = readonly;
		this.startTime = starttime;
		this.status = 0;
		this.operations = new LinkedList<Message>();
		this.readvalues = new LinkedList<int[]>();
	}
	public void addOperation(Message msg)
	{
		this.operations.add(msg);
	}
	public void waitTransaction()
	{
		this.status = 1;
	}
	public void endTransaction()
	{
		this.status = 2;
		this.operations.clear();
		this.readvalues.clear();
		this.ReadOnly = false;
	}
	public void abortTransaction()
	{
		this.status = 3;
		this.operations.clear();
		this.readvalues.clear();
		this.ReadOnly = false;
	}
	public void renewTransaction(int newtimer)
	{
		this.status = 0;
		this.startTime = newtimer;
	}
}

//transaction status:
//	0 = new and fine
//	1 = wait
//	2 = committed 
//	3 = abort
