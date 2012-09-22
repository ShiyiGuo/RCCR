package Transaction;

import java.util.*;
import java.util.List;
import java.util.Vector;

import Site.Site;

public class TransManager{
	public Vector<Transaction> translist;
	public List<List<Integer>> sitebuffer;
	
	public TransManager()
	{
		this.translist = new Vector<Transaction>();
		this.sitebuffer = new ArrayList<List<Integer>>();
	}
	
	public List<Message> parser(String input,int timer)
	{
		List<Message> optlist = new LinkedList<Message>();
		String[] operations = input.split(";");
		for(String operation:operations)
		{
			Message msg = null;
			if(operation.startsWith("begin("))
			{
				String Tname = operation.substring(operation.indexOf("("),operation.indexOf(")"));
				int transId = Integer.parseInt(Tname.substring(1));
				msg = new Message(transId,Command.BEGIN);
			}
			else if(operation.startsWith("end"))
			{
				String Tname = operation.substring(operation.indexOf("("),operation.indexOf(")"));
				int transId = Integer.parseInt(Tname.substring(1));
				msg = new Message(transId,Command.BEGIN);
			}
			else if(operation.startsWith("dump"))
			{
				msg = new Message(Command.DUMP);
			}
			else if(operation.startsWith("recover"))
			{
				String Tname = operation.substring(operation.indexOf("("),operation.indexOf(")"));
				int siteId = Integer.parseInt(Tname);
				msg = new Message(Command.RECOVER,siteId);				
			}
			else if(operation.startsWith("fail"))
			{
				String Tname = operation.substring(operation.indexOf("("),operation.indexOf(")"));
				msg = new Message(Command.FAIL,Integer.parseInt(Tname));
			}
			else if(operation.startsWith("beginRO"))
			{
				String Tname = operation.substring(operation.indexOf("("),operation.indexOf(")"));
				msg = new Message(Command.RO,Integer.parseInt(Tname));
			}
			else
			{
				if(operation.startsWith("R"))
				{
					String pair = operation.substring(operation.indexOf("("),operation.indexOf(")"));
					String[] pairs = pair.split(",");
					int transId = Integer.parseInt(pairs[0].substring(1));
					int varId = Integer.parseInt(pairs[1].substring(1));
					msg = new Message(transId,Command.READ,varId);
				}
				else if(operation.startsWith("W"))
				{
					String pair = operation.substring(operation.indexOf("("),operation.indexOf(")"));
					String[] pairs = pair.split(",");
					int transId = Integer.parseInt(pairs[0].substring(1));
					int varId = Integer.parseInt(pairs[1].substring(1));
					int varValue = Integer.parseInt(pairs[2]);
					msg = new Message(transId,Command.READ,varId,varValue);
				}
			}
			optlist.add(msg);
		}
		return optlist;
	}
	
	public void dispatcher(Message msg,int timer,List<Site> sitelist,int source)
	{
		switch (msg.command)
		{
		case BEGIN:
			translist.add(new Transaction(msg.transId,false,timer));
			return;
		case END:
		{
			assert(translist.get(msg.transId) != null);			
			if(translist.get(msg.transId).readvalues.size() != 0)
			{
				for(int[] values:translist.get(msg.transId).readvalues)
				{
					System.out.println("variable "+values[0]+"="+values[1]);
				}
			}
			if(!translist.get(msg.transId).ReadOnly)
			{
				Message endmsg = new Message(msg.transId,Command.END);
				for(Site site:sitelist)
				{
					site.processRequest(endmsg);
				}
				translist.get(msg.transId).endTransaction();
				for(Transaction Ti:translist)//process waiting transactions after one commited transaction
				{
					if(Ti.status == 1)
					{
						Ti.status = 0;
						for(Message bufmsg:Ti.operations)
						{
							dispatcher(bufmsg,timer,sitelist,1);
						}
						if(Ti.operations.size() != 0)
						{
							Ti.status = 1;
						}
					}
				}
			}	
		}
		return;
		case READ:
			//TODO: run site function of read
		{
			assert(translist.get(msg.transId) != null);
	
			Message result = new Message();
			if(translist.get(msg.transId).status == 0)
			{//if transaction status is good, take the action
				//TODO: add transfer operation to site and logic of next step
				result = read(msg,sitelist,translist.get(msg.transId).ReadOnly);
			}else if(translist.get(msg.transId).status == 1)
			{//if transaction is waiting, add operation to buffer list
				translist.get(msg.transId).addOperation(msg);
				return;
			}else{
			//if transaction is aborted or committed already, renew the transaction and take action
				translist.get(msg.transId).renewTransaction(timer);
				//TODO: add transfer operation to site and logic of next step
				result = read(msg,sitelist,translist.get(msg.transId).ReadOnly);
			}
			if(!result.retstatus.equals(Message.return_status.SUCCESS) && source == 0)
			{
				if(result.retstatus.equals(Message.return_status.CONFLICT))
				{//confliction: wait or die
					Command cmd = waitordie(msg.transId,result);
					switch (cmd)
					{
					case WAIT:
					{
						translist.get(msg.transId).operations.add(msg);
						translist.get(msg.transId).waitTransaction();
					}
					return;
					case ABORT:
						abort(msg.transId,sitelist);
					return;
					default:
					return;
					}
				}else{//site fail: always wait
					translist.get(msg.transId).operations.add(msg);
					translist.get(msg.transId).waitTransaction();
				}
			}else if(result.retstatus.equals(Message.return_status.SUCCESS))
			{
				int[] resultvalue = new int[2];
				resultvalue[0] = msg.varId;
				resultvalue[1] = result.retrun_value;
				translist.get(msg.transId).readvalues.add(resultvalue);
				if(source == 1)//remove waiting operations
				{
					for(Message buffmsg:translist.get(msg.transId).operations)
					{
						if(buffmsg.equals(msg))
						{
							translist.get(msg.transId).operations.remove(buffmsg);
							break;
						}
					}
				}
			}			
		
		}
		return;
		case RO:
		{
			translist.add(new Transaction(msg.transId,true,timer));
			Message rresult = read(msg,sitelist,true);
			if(rresult.retstatus.equals(Message.return_status.SUCCESS))
			{
				int[] values = new int[2];
				values[0] = msg.varId;
				values[1] = rresult.retrun_value;
				translist.get(msg.transId).readvalues.add(values);
				if(source == 1)//remove waiting operations
				{
					for(Message buffmsg:translist.get(msg.transId).operations)
					{
						if(buffmsg.equals(msg))
						{
							translist.get(msg.transId).operations.remove(buffmsg);
							break;
						}
					}
				}
			}else{
				if(source == 0)
				{
					translist.get(msg.transId).operations.add(msg);
					translist.get(msg.transId).waitTransaction();
				}
			}
		}
		return;
		case WRITE:
		{
			//translist.get(msg.transId).addOperation(msg);
			assert(translist.get(msg.transId) != null);
		
			Message wresult = new Message();
			if(translist.get(msg.transId).status == 0)
			{
				wresult = write(msg,sitelist);
			}else if(translist.get(msg.transId).status == 1)
			{
				translist.get(msg.transId).operations.add(msg);
				return;
			}else{
				translist.get(msg.transId).renewTransaction(timer);
				wresult = write(msg,sitelist);
			}
			if(!wresult.retstatus.equals(Message.return_status.SUCCESS)&& source == 0)
			{
				if(wresult.retstatus.equals(Message.return_status.CONFLICT))
				{
					Command cmd = waitordie(msg.transId,wresult);
					switch (cmd)
					{
					case WAIT:
					{
						translist.get(msg.transId).operations.add(msg);
						translist.get(msg.transId).waitTransaction();
					}
					return;
					case ABORT:
						abort(msg.transId,sitelist);
					return;
					default:
						System.out.println("unknown action");
					return;
					}
				}else{
					translist.get(msg.transId).operations.add(msg);
					translist.get(msg.transId).waitTransaction();
				}
			}else if(wresult.retstatus.equals(Message.return_status.SUCCESS) && source == 1)
			{
				for(Message buffmsg:translist.get(msg.transId).operations)
				{
					if(buffmsg.equals(msg))
					{
						translist.get(msg.transId).operations.remove(buffmsg);
						break;
					}
				}
			}
		
		}
		return;
		case FAIL:
			//abort all the transaction with lock on this site
			for(int transid:sitebuffer.get(msg.siteId))
			{
				if(translist.get(transid).status != 2 && !translist.get(transid).ReadOnly)
				{
					translist.get(transid).abortTransaction();
					Message abortmsg = new Message(transid,Command.ABORT);
					for(Site site:sitelist)
					{
						abortmsg = site.processRequest(abortmsg);
					}
				}
			}
			Message failmsg = new Message(Command.FAIL,msg.siteId);
			sitelist.get(msg.siteId).processRequest(failmsg);
			sitebuffer.get(msg.siteId).clear();
			return;
		case RECOVER:
			//TODO: run site function recover
		{
			Message recovermsg = new Message(Command.RECOVER,msg.siteId);
			recovermsg = sitelist.get(msg.siteId).processRequest(recovermsg);
			for(Transaction waittrans:translist)
			{
				if(waittrans.status == 1)
				{
					for(Message waitmsg:waittrans.operations)
					{
						dispatcher(waitmsg,timer,sitelist,1);
					}
				}
			}
		}
			return;
		case DUMP:
		{
			Message dumpmsg = new Message(Command.DUMP);
			for(Site site:sitelist)
			{
				Message resultmsg = site.processRequest(dumpmsg);
			}
		}
			return;
		}
	}
	
	private Message read(Message msg,List<Site> sitelist,boolean readonly)
	{
		Message result = new Message();
		Message confres = null,failres = null;
		if(msg.varId%2 == 0)
		{
			for(int i=0;i<sitelist.size();++i)
			{
				Site tmpsite = sitelist.get(i);
				result = tmpsite.processRequest(msg);
				switch (result.retstatus)
				{
				case SITEFAIL:
					failres = result;
					continue;
				case CONFLICT:
					confres = result;
					continue;
				default:
				{
					sitebuffer.get(i).add(msg.transId);
					return result;
				}
				}
			}
			if(confres != null)
			{
				return confres;
			}else{
				return failres;
			}
		}
		else
		{
			int siteid = msg.varId%10 + 1;
			result = sitelist.get(siteid).processRequest(msg);
			if(result.retstatus.equals(Message.return_status.SUCCESS))
			{
				sitebuffer.get(siteid).add(msg.transId);
			}
		}
		return result;
	}
	
	private Message write(Message msg,List<Site> sitelist)
	{
		Message result = new Message();
		Message confres=null,failres=null,succres = null;
		if(msg.varId%2 == 0)
		{
			for(int i=0;i<sitelist.size();++i)
			{
				Site tmpsite = sitelist.get(i);
				result = tmpsite.requireLock(msg);
				
				switch (result.retstatus)
				{
				case SITEFAIL:
					failres = result;
					continue;
				case CONFLICT:
					confres = result;
					break;
				default:
					succres = result;
				}
			}
			if(confres != null)
			{
				return confres;
			}else{
				for(Site tmpsite:sitelist)
				{
					result = tmpsite.processRequest(msg);
				}
				if(succres != null)
				{
					return succres;
				}else{
					return result;
				}
			}
		}
		else
		{
			int siteid = msg.varId%10 + 1;
			result = sitelist.get(siteid).processRequest(msg);
			
			if(result.retstatus.equals(Message.return_status.SUCCESS))
			{
				sitebuffer.get(siteid).add(msg.transId);
			}
		}
		return result;
	}
	
	private void abort(int transid,List<Site> sitelist)
	{		
		translist.get(transid).abortTransaction();
		Message abortmsg = new Message(transid,Command.ABORT);
		for(Site site:sitelist)
		{
			site.processRequest(abortmsg);
		}
	}
	
	private Command waitordie(int transid,Message result)
	{
		boolean youngest = true;
		for(int Ti:result.conflictT)
		{
			if(translist.get(Ti).startTime > translist.get(result.transId).startTime)
			{
				youngest = false;
			}
		}
		if(youngest)//abort youngest transaction 
		{
			return Command.ABORT;
		}else{
			return Command.WAIT;
		}
	}
}
