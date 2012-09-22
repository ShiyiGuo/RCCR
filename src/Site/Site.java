package Site;
import Transaction.Message;

public class Site {
	public Message requireLock(Message msg)
	{
		return new Message();
	}
	public Message processRequest(Message msg)      
	{    
		return new Message();
//		Message res = new Message();              
//		res.msgt = msgtype.RETURN;              
//		switch(msg.command)             
//		{                       
//		case FAIL:                           
//			res.return_status = setFailed();                                
//			break;                  
//		case RECOVER:                                
//			res.return_status = setRecovered();                             
//			break;                  
//		case READ:                           
//			res.retrun_value = Read(msg);                           
//			break;                  
//		case WRITE:                          
//			res.return_status = Write(msg);                         
//			break;                  
//		case RO:                             
//			break;                  
//		case END:                               
//			res.return_status = commit(msg);                        
//		case DUMP:                           
//			dump();                 
//		default:                                
//			assert(true);                                   
//			break;          
//		}               
//		return res;    
	}
}
