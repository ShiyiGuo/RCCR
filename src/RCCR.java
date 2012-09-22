import java.util.*;
import java.io.*;
import Transaction.*;
import Site.*;

public class RCCR {
	private TransManager tm;
	private List<Site> sitelist;
	private int timer;
	
	//-----class definition
	public RCCR()
	{
		//initiate transaction manager, data manager, sites and variables
		this.tm = new TransManager();
		this.sitelist = new ArrayList<Site>();
		this.timer = 0;
		
		for(int i=0;i<10;i++)
		{//TODO add argument to site construction function
			this.sitelist.add(new Site());
		}
		
	}
	
	void run(String infile) throws FileNotFoundException
	{
		FileInputStream fstr = new FileInputStream(infile);
		DataInputStream dstr = new DataInputStream(fstr);
		BufferedReader breader = new BufferedReader(new InputStreamReader(dstr));
		String inline = "";
		boolean begin = false;
		try {
			while((inline = breader.readLine()) != null)
			{//TODO main logic
				//process the current command line
				if(!begin && inline.startsWith("begin"))
				{
					begin = true;
				}
				if(begin)
				{
					if(inline.equals(""))
					{
						timer++;
					}else{
						List<Message> optlist = tm.parser(inline.trim(),timer);
						for(Message opt:optlist)
						{
							tm.dispatcher(opt,timer,sitelist,0);
						}
						//TODO: process waiting operations
//						for(Message opt:tm.buffoper)
//						{
//							tm.dispatcher(opt, timer, sitelist,1);
//						}
						timer++;
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	//-----main function
	public static void main(String[] args) throws FileNotFoundException
	{
		String infile = args[0];
		RCCR rccr = new RCCR();
		rccr.run(infile);
	}
	
}
