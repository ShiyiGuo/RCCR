package Transaction;

import java.util.*;
public class Message {
	public int transId;
	public int varId;
	public int varValue;
	public int siteId;
	public Command command;
	enum return_status {SUCCESS, CONFLICT, SITEFAIL};
	public return_status retstatus;
	public int retrun_value;
	public List<Integer> conflictT;
	
	public Message(){}

	public Message(int transid,Command command)
	{	
		this.transId = transid;
		this.command = command;
	}
	public Message(Command command,int siteid)
	{
		this.siteId = siteid;
		this.command = command;
	}
	public Message(Command command)
	{
		this.command = command;
	}
	public Message(int transid,Command command,int varid)
	{
		this.command = command;
		this.transId = transid;
		this.varId = varid;
	}
	public Message(int transid,Command command,int varid,int varvalue)
	{
		this.command = command;
		this.transId = transid;
		this.varId = varid;
		this.varValue = varvalue;
	}
}
