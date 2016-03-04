package gs.hey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, Text>
{
	String line;
	String smsText;
	String smsid;
	String time;
	String phoneno;
	String userid;
	String circle;
	String sms[]=new String [20];
	int priority=0;
	String val=" ";
	String attribute=" ";
	String st;
	String substring[]=new String [20];
	
	
 public void map(LongWritable key, Text value, Context context) throws IOException
      {
    line = value.toString();
    sms = line.split("\\|");
    smsText=sms[13];
    smsid=sms[0];
    time=sms[10];
    phoneno=sms[3];
    userid=sms[2];
    circle=sms[9];
    	
    st=smsText.toLowerCase();
	st=st+" ";
	substring=st.split(" ");
	WCMapper wc =new WCMapper();
	  	 
    wc.location(st,smsid,phoneno,time,context,circle);
    wc.credit_card(st,smsid,phoneno,time, context);
    wc.life_insurance(st,smsid,phoneno,time,context);
    wc.health_insurance(st,smsid,phoneno,time,context);
    wc.mutual_fund(st,smsid,phoneno,time,context);
    wc.home_loan(st,smsid,phoneno,time,context);
    wc.internet_banking(st,smsid,phoneno,time,context);
    wc.gender(st,smsid,phoneno,time,context);
    wc.has_kids(st,smsid,phoneno,time,context);
    wc.savings_account(st,smsid,phoneno,time,context);
    wc.income(st,smsid,phoneno,time,context);
       }
      public void location(String st,String smsid,String phoneno,String time,Context context,String circle) throws IOException
      {
    	 String smstxt=st;
     	 String sid=smsid;
     	 String pno=phoneno;
     	 String t=time;
     	 String cir=circle;
     	 
     	 if(cir.equals("Mumbai"))
     	 {
     		 priority=1;
     		 attribute="location";
     		 val="Mumabi";
     		try {
				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
     	 }
     	 if(cir.equals("Kolkata"))
     	 {
     		 priority=1;
     		 attribute="location";
     		 val="Kolkata";
     		try {
				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
     	 }
     	if(cir.equals("Chennai"))
    	 {
    		 priority=1;
    		 attribute="location";
    		 val="Chennai";
    		try {
				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	 }
    Map<String, List<String>> map = new HashMap<String, List<String>>();                //finding location attribute
    	  
          // create list one and store values
    List<String> valSetOne = new ArrayList<String>();
    valSetOne.add("pune");
    valSetOne.add("nagpur");
   
          // create list two and store values
    List<String> valSetTwo = new ArrayList<String>();
    valSetTwo.add("udaipur");
    valSetTwo.add("jodhpur");
    valSetTwo.add("jaisalmer");
    valSetTwo.add("jaipur");
  	valSetTwo.add("ajmer");
  	valSetTwo.add("bikaner");
  	valSetTwo.add("kota");
   
          // create list three and store values
    List<String> valSetThree = new ArrayList<String>();
    valSetThree.add("bangalore");
  	valSetThree.add("mysore");
    valSetThree.add("ooty");

  	List<String> valSetFour = new ArrayList<String>();
    valSetThree.add("patna");
  	valSetThree.add("muzzafarnagar");
          
  	List<String> valSetFive = new ArrayList<String>();
    valSetThree.add("ludhiana");
  	valSetThree.add("patiala");
    valSetThree.add("bhatinda");
  	valSetThree.add("hoshiyarpur");

  	List<String> valSetSix = new ArrayList<String>();
    valSetThree.add("bhubaneswar");
  	valSetThree.add("cuttack");
          
  	List<String> valSetSeven = new ArrayList<String>();
    valSetThree.add("coimbatore");
  	valSetThree.add("madurai");
          
    List<String> westbengal = new ArrayList<String>();
    westbengal.add("durgapur");
    westbengal.add("howrah");
    westbengal.add("darjeeling");
    westbengal.add("siliguri");
    westbengal.add("asansol");
    westbengal.add("midnapore");
    westbengal.add("bolpur");
    westbengal.add("kalyani");

  	List<String> valSetNine = new ArrayList<String>();
    valSetThree.add("surat");
  	valSetThree.add("ahmedabad");
  	valSetThree.add("gandhinagar");
  	valSetThree.add("rajkot");
  	
  	List<String> madhyapradesh = new ArrayList<String>();
  	madhyapradesh.add("bhopal");
  	madhyapradesh.add("indore");
  	madhyapradesh.add("jabalpur");
  	madhyapradesh.add("ujjain");
  	madhyapradesh.add("gwalior");
  	madhyapradesh.add("ratlam");
  	madhyapradesh.add("neemuch");
  	
  	List<String> kerala = new ArrayList<String>();
    kerala.add("kochi");
  	kerala.add("thiruvananthapuram");
  	
  	List<String> andhrapradesh = new ArrayList<String>();
  	andhrapradesh.add("hyderabad");
  	andhrapradesh.add("vijayawada");
  	andhrapradesh.add("secunderabad");
  //	andhrapradesh.add("rajkot");
  	
  	
  	List<String> jammu = new ArrayList<String>();
    valSetThree.add("srinagar");
  	valSetThree.add("jammu");
  	
	List<String> assam = new ArrayList<String>();
	assam.add("guwahati");
	assam.add("silchar");
	assam.add("jorhat");
	assam.add("tezpur");
	assam.add("dibrugarh");
  	
  	List<String> uttarpradesheast = new ArrayList<String>();
  	uttarpradesheast.add("lucknow");
  	uttarpradesheast.add("kanpur");
  	uttarpradesheast.add("varanasi");
  	uttarpradesheast.add("gorakhpur");
  	uttarpradesheast.add("allahabad");
  	uttarpradesheast.add("ajamgarh");
  	uttarpradesheast.add("faizabad");
  	uttarpradesheast.add("moradabad");
  	uttarpradesheast.add("rampur");
  	uttarpradesheast.add("barelly");
  	uttarpradesheast.add("bareilly");
  	uttarpradesheast.add("jhansi");
  	uttarpradesheast.add("banaras");
  	uttarpradesheast.add("mathura");
  	uttarpradesheast.add("barabanki");
  	uttarpradesheast.add("sultanpur");
  	uttarpradesheast.add("saharanpur");
  	uttarpradesheast.add("sitapur");
  	uttarpradesheast.add("lakhimpur");
  	uttarpradesheast.add("pilibhit");
  	uttarpradesheast.add("hardoi");
  	      
          // put values into map
    map.put("Maharashtra", valSetOne);
    map.put("Rajasthan", valSetTwo);
    map.put("Karnataka", valSetThree);
  	map.put("Bihar", valSetFour);
  	map.put("Punjab", valSetFive);
  	map.put("Orissa", valSetSix);
  	map.put("Tamilnadu", valSetSeven);
  	map.put("WestBengal", westbengal);
  	map.put("Gujarat", valSetNine);
  	map.put("Jammu", jammu);
  	map.put("Assam", assam);
  	map.put("Kerala", kerala);
  	map.put("UttarPradeshEast", uttarpradesheast);
  	map.put("AndhraPradesh", andhrapradesh);
  	map.put("MadhyaPradesh", madhyapradesh);

  	
          for (Map.Entry<String, List<String>> entry : map.entrySet()) {
              String keyss = entry.getKey();
  	    if(cir.equals(keyss))
  	    {
              List<String> values = entry.getValue();
  	    for(String city:values)
  	    {
              if(smstxt.contains(city) && !smstxt.contains(city +"road"))
              {
  	    priority=2;
  	    attribute="location";
  	    val=city;
  	    try {
  				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
  	  	
  			} catch (InterruptedException e) {
  		
  				e.printStackTrace();
  			} 
  		
  	    }
              }
  	}
  	} 	  
      }
     public void credit_card(String st,String smsid,String phoneno,String time,Context context) throws IOException
      {
    	 String smstxt=st;
    	 String sid=smsid;
    	 String pno=phoneno;
    	 String t=time;
    	  if(smstxt.contains("credit card") || smstxt.contains("creditcard"))                            // credit card attribute
    		{
    		priority=1;
    		attribute="credit card";
    		val="Yes";
    		try {
    				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
    		
    			} catch (InterruptedException e) {
    		
    				e.printStackTrace();
    			} 

    		}
    	  if(smstxt.contains("card") && (smstxt.contains("payment") || smstxt.contains("receive")|| smstxt.contains("changed")|| smstxt.contains("blocked")|| smstxt.contains("resolved")))                            // credit card attribute
  		{
  		priority=2;
  		attribute="credit card";
  		val="Yes";
  		try {
  				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
  		
  			} catch (InterruptedException e) {
  		
  				e.printStackTrace();
  			} 

  		}
    	  if(smstxt.contains("card") && smstxt.contains("transaction") && smstxt.contains("declined"))                            // credit card attribute
    		{
    		priority=2;
    		attribute="credit card";
    		val="Yes";
    		try {
    				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
    		
    			} catch (InterruptedException e) {
    		
    				e.printStackTrace();
    			} 

    		}
    	  

      }
     
     public void savings_account(String st,String smsid,String phoneno,String time,Context context) throws IOException
     {
   	  	  String smstxt=st;
    	  String sid=smsid;
    	  String pno=phoneno;
    	  String t=time;
    	 
   	  if(smstxt.contains("savings a/c") || smstxt.contains("savings acct")|| smstxt.contains("savings no")|| smstxt.contains("saving account")|| smstxt.contains("savings acct")|| smstxt.contains("savings transactions")|| smstxt.contains("savings available balance")||(smstxt.contains("salary")&&smstxt.contains("credited"))||smstxt.contains("mobile banking"))                                                     //life insurance
   		{
   		priority=1;
   		attribute="savings account";
   		val="Yes";
   		try {
   				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
   		
   			} catch (InterruptedException e) {
   		
   				e.printStackTrace();
   			} 

   		}
   	  
   	 if(smstxt.contains("credit card") || smstxt.contains("creditcard"))                                                     //life insurance
		{
		priority=2;
		attribute="savings account";
		val="Yes";
		try {
				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
		
			} catch (InterruptedException e) {
		
				e.printStackTrace();
			} 

		}
   	if(smstxt.contains("remittance account rejected") || smstxt.contains("last 4 txns"))                                                     //life insurance
	{
	priority=3;
	attribute="savings account";
	val="Yes";
	try {
			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
	
		} catch (InterruptedException e) {
	
			e.printStackTrace();
		} 

	}
   	  
   	  
   	  
   	  
     }

     public void has_kids(String st,String smsid,String phoneno,String time,Context context) throws IOException
     {
   	  	  String smstxt=st;
    	  String sid=smsid;
    	  String pno=phoneno;
    	  String t=time;
    	 
   	  if(smstxt.contains("dear parent")||smstxt.contains("your child")||(smstxt.contains("school")&&!smstxt.contains("staff"))||smstxt.contains("quarter fee")||smstxt.contains("fee of your ward"))                                                     //life insurance
   		{
   		priority=1;
   		attribute="has kids";
   		val="Yes";
   		try {
   				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
   		
   			} catch (InterruptedException e) {
   		
   				e.printStackTrace();
   			} 

   		}
     }
     public void income(String st,String smsid,String phoneno,String time,Context context) throws IOException
     {
   	  String smstxt=st;
    	  String sid=smsid;
    	  String pno=phoneno;
    	  String t=time;
    	 
   	  if(smstxt.contains("Salary")&&smstxt.contains("is credited with INR"))                                                     //life insurance
   		{
   		priority=1;
   		attribute="life insurance";
   		val="Yes";
   		String s = smstxt.substring(smstxt.indexOf("is credited with INR") + 1, smstxt.indexOf(" on"));
   		int salaryamt = Integer.parseInt(s);
   		int spy=salaryamt*12;
   		
   		try {
   				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
   		
   			} catch (InterruptedException e) {
   		
   				e.printStackTrace();
   			} 

   		}
     }
     
     public String range(int amt)
     {
    	 int salary=amt;
    	 int spy=salary*12;
    	 if(spy>=0 && spy <=300000)
    	 {
    		 return "0-3L";
    	 }
    	 else if(spy>300000 && spy <=600000)
    	 {
    		 return "3-6L";
    	 }
    	 else if(spy>600000 && spy <=1000000)
    	 {
    		 return "6-10L";
    	 }
    	 else if(spy>1000000 && spy <=1500000)
    	 {
    		 return "10-15L";
    	 }
    	 else if(spy>1500000 && spy<=2000000)
    		 return "15-20L";
    	 else if(spy>2000000 && spy<=2500000)
    		 return "20-25L";
    	 else if(spy>2500000 && spy<=4000000)
    		 return "25-40L";
    	 else if(spy>4000000 && spy<=6000000)
    		 return "40-60L";
    	 else if(spy>6000000 && spy <=10000000)
    		 return "60L-1CR";
    	 else
    		 return "1CR+";
    	 
    
    	 
     }
     
     
      public void life_insurance(String st,String smsid,String phoneno,String time,Context context) throws IOException
      {
    	  String smstxt=st;
     	  String sid=smsid;
     	  String pno=phoneno;
     	  String t=time;
     	 
    	  if(smstxt.contains("life insurance"))                                                     //life insurance
    		{
    		priority=1;
    		attribute="life insurance";
    		val="Yes";
    		try {
    				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
    		
    			} catch (InterruptedException e) {
    		
    				e.printStackTrace();
    			} 

    		}
      }
      public void health_insurance(String st,String smsid,String phoneno,String time,Context context) throws IOException
      {
    	  String smstxt=st;
     	  String sid=smsid;
     	  String pno=phoneno;
     	  String t=time;
     	 
    	  if(smstxt.contains("health insurance") || smstxt.contains("medical insurance") || userid=="religarexml")
    		{
    		priority=1;
    		attribute="health insurance";
    		val="Yes";
    		try {
    				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
    		
    			} catch (InterruptedException e) {
    		
    				e.printStackTrace();
    			} 

    		}
    		  
    	  }
    	  
public void mutual_fund(String st,String smsid,String phoneno,String time,Context context) throws IOException
{
	  String smstxt=st;
	  String sid=smsid;
	  String pno=phoneno;
	  String t=time;
	 
      if(smstxt.contains("mutual fund")||(smstxt.contains("folio number")&&smstxt.contains("request"))||smstxt.contains("units in folio"))                                                     //mutual fund
  	{
  	priority=1;
  	attribute="mutual fund";
  	val="Yes";
  	try {
  			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
  	
  		} catch (InterruptedException e) {
  	
  			e.printStackTrace();
  		} 

  	}

      
}      
public void home_loan(String st,String smsid,String phoneno,String time,Context context) throws IOException
{
	  String smstxt=st;
	  String sid=smsid;
	  String pno=phoneno;
	  String t=time;
	 
	if(smstxt.contains("Home Loan"))                                                     //home loan
	{
	priority=1;
	attribute="home loan";
	val="Yes";
	try {
			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
	
		} catch (InterruptedException e) {
	
			e.printStackTrace();
		} 

	}

   
      
}  
public void internet_banking(String st,String smsid,String phoneno,String time,Context context) throws IOException
{
	  String smstxt=st;
	  String sid=smsid;
	  String pno=phoneno;
	  String t=time;
	 
	if(smstxt.contains("internet banking") || smstxt.contains("inter net bkg.") || smstxt.contains("netbanking") || smstxt.contains("e_banking") || smstxt.contains("net banking"))      // net banking
	{
	priority=1;
	attribute="net banking";
	val="Yes";
	try {
			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
	
		} catch (InterruptedException e) {
	
			e.printStackTrace();
		} 

	}    
}      
public void gender(String st,String smsid,String phoneno,String time,Context context) throws IOException
{
	  String smstxt=st;
	  String sid=smsid;
	  String pno=phoneno;
	  String t=time;
	 
	   if(smstxt.startsWith("dear mr.")||smstxt.startsWith("dear sir")||smstxt.startsWith("dear mr.")||smstxt.startsWith("dear salesperson")||smstxt.startsWith("dear mr."))// gender attribute
 	    {
 	    	priority=1;
 	    	val="male";
 	    	attribute="gender";
 	    	try {
 				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
 	  	
 			} catch (InterruptedException e) {
 		
 				e.printStackTrace();
 			} 
 	    }
 	    else if(smstxt.startsWith("hi sir")||smstxt.startsWith("hi uncle")||smstxt.startsWith("hi bhaiya")||smstxt.startsWith("hi bro")||smstxt.startsWith("hi brother")||smstxt.startsWith("hi father")||smstxt.startsWith("hi papa")||smstxt.startsWith("hi dad")||smstxt.startsWith("hi pa")||smstxt.startsWith("hi dady")||smstxt.startsWith("hi jiju")||smstxt.startsWith("hi mama")||smstxt.startsWith("hi chacha")||smstxt.startsWith("hi tau")||smstxt.startsWith("hi bhai")||smstxt.startsWith("hi ladke")||smstxt.startsWith("hi mr."))
 	    {
 	    	priority=1;
 	    	val="male";
 	    	attribute="gender";
 	    	try {
 				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
 	  	
 			} catch (InterruptedException e) {
 		
 				e.printStackTrace();
 			} 
 	    }
 	    else if(smstxt.startsWith("hi mumma")||smstxt.startsWith("hi mummy")||smstxt.startsWith("hi mom")||smstxt.startsWith("hi di")||smstxt.startsWith("hi sis")||smstxt.startsWith("hi didi")||smstxt.startsWith("hi bua")||smstxt.startsWith("hi bhabhi")||smstxt.startsWith("hi behen ")||smstxt.startsWith("hi behna")||smstxt.startsWith("hi behenji")||smstxt.startsWith("hi aunty")||smstxt.startsWith("hi chachi")||smstxt.startsWith("hi ladki")||smstxt.startsWith("hi mam")||smstxt.startsWith("hi madam")){
 	    	priority=1;
 	    	val="female";
 	    	attribute="gender";
 	    	try {
 				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
 	  	
 			} catch (InterruptedException e) {
 		
 				e.printStackTrace();
 			} 
 	    }
 	    else if(smstxt.startsWith("hey sir")||smstxt.startsWith("hey uncle")||smstxt.startsWith("hey bhaiya")||smstxt.startsWith("hey bro")||smstxt.startsWith("hey didi")||smstxt.startsWith("hey brother")||smstxt.startsWith("hey father")||smstxt.startsWith("hey papa")||smstxt.startsWith("hey dad")||smstxt.startsWith("hey pa")||smstxt.startsWith("hi dady")||smstxt.startsWith("hey jiju")||smstxt.startsWith("hey mama")||smstxt.startsWith("hey chacha")||smstxt.startsWith("hey tau")||smstxt.startsWith("hey bhai")||smstxt.startsWith("hey ladke"))
 	    {
 	    	priority=1;
 	    	val="male";
 	    	attribute="gender";
 	    	try {
 				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
 	  	
 			} catch (InterruptedException e) {
 		
 				e.printStackTrace();
 			} 
 	    }
 	  else if(smstxt.startsWith("hello sir")||smstxt.startsWith("hello uncle")||smstxt.startsWith("hello bhaiya")||smstxt.startsWith("hello bro")||smstxt.startsWith("hello brother")||smstxt.startsWith("hello father")||smstxt.startsWith("hello papa")||smstxt.startsWith("hello dad")||smstxt.startsWith("hello pa ")||smstxt.startsWith("hello dady")||smstxt.startsWith("hello jiju")||smstxt.startsWith("hello mama")||smstxt.startsWith("hello chacha")||smstxt.startsWith("hello tau")||smstxt.startsWith("hello bhai")||smstxt.startsWith("hello ladke"))
	    {
	    	priority=1;
	    	val="male";
	    	attribute="gender";
	    	try {
				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
	  	
			} catch (InterruptedException e) {
		
				e.printStackTrace();
			} 
	    }
 	
 	else if(smstxt.startsWith("hey mumma")||smstxt.startsWith("hey mummy")||smstxt.startsWith("hey mom")||smstxt.startsWith("hey di")||smstxt.startsWith("hey sis")||smstxt.startsWith("hey didi")||smstxt.startsWith("hey bua")||smstxt.startsWith("hey bhabhi")||smstxt.startsWith("hey behen ")||smstxt.startsWith("hey behna")||smstxt.startsWith("hey behenji")||smstxt.startsWith("hey aunty")||smstxt.startsWith("hey chachi")||smstxt.startsWith("hey ladki")||smstxt.startsWith("hey mam")||smstxt.startsWith("hey madam")||smstxt.startsWith("hey madam")){
	    	priority=1;
	    	val="female";
	    	attribute="gender";
	    	try {
				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
	  	
			} catch (InterruptedException e) {
		
				e.printStackTrace();
			} 
	    }
 	else if(smstxt.startsWith("dear mrs.")||smstxt.startsWith("dear mam")||smstxt.startsWith("dear madam")||smstxt.startsWith("dear ms."))
	    {
	    	priority=1;
	    	val="female";
	    	attribute="gender";
	    	try {
				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
	  	
			} catch (InterruptedException e) {
		
				e.printStackTrace();
			} 
	    }
 	    
 	else if(smstxt.startsWith("aur mote")||smstxt.startsWith("aur londe"))
    {
    	priority=1;
    	val="male";
    	attribute="gender";
    	
    	try {
			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
  	
		} catch (InterruptedException e) {
	
			e.printStackTrace();
		} 
    }
 	else if(smstxt.startsWith("aur moti"))
    {
    	priority=1;
    	val="female";
    	attribute="gender";
    	
    	try {
			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
  	
		} catch (InterruptedException e) {
	
			e.printStackTrace();
		} 
    	
    }
/*  	else if((st.startsWith("dear")||st.startsWith("hi")||st.startsWith("hello")||st.startsWith("hey"))&&(substring[1]!="customer")&&(substring[1]!="executive")&&(substring[1]!="student")){
 		
 	int	l=substring[1].length();
 	if(substring[1].charAt(l-1)=='a'||substring[1].charAt(l-1)=='e'||substring[1].charAt(l-1)=='i'||substring[1].charAt(l-1)=='o'||substring[1].charAt(l-1)=='u')
 		val="female";
 	else
 		val="male";
 	attribute="gender";
 	priority=2;
 	
 	try {
		context.write(new Text(smsid), new Text(phoneno+"\t"+time+"\t"+attribute+"\t"+val+"\t"+priority));
	
	} catch (InterruptedException e) {

		e.printStackTrace();
	} 
 	    	    }
*/

	
 
      
}      
      
}