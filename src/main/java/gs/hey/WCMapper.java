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
	String fromnumber;
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
    fromnumber=sms[4];
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
    wc.has_kids(st,smsid,phoneno,time,fromnumber,context);
    wc.savings_account(st,smsid,phoneno,time,context);
    wc.car_loan(st,smsid,phoneno,time,context);
  //  wc.income(st,smsid,phoneno,time,context);
    wc.age(st,smsid,phoneno,time,context);
    wc.car_insurance(st,smsid,phoneno,time,context);
       }
      public void location(String smstxt,String sid,String pno,String t,Context context,String cir) throws IOException
      { 
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
    List<String> maharashtra = new ArrayList<String>();
    maharashtra.add("pune");
    maharashtra.add("nagpur");
   
          // create list two and store values
    List<String> rajasthan = new ArrayList<String>();
    rajasthan.add("udaipur");
    rajasthan.add("jodhpur");
    rajasthan.add("jaisalmer");
    rajasthan.add("jaipur");
  	rajasthan.add("ajmer");
  	rajasthan.add("bikaner");
  	rajasthan.add("kota");
   
          // create list three and store values
    List<String> karnataka = new ArrayList<String>();
    karnataka.add("bangalore");
  	karnataka.add("mysore");
    karnataka.add("ooty");

  	List<String> bihar = new ArrayList<String>();
    bihar.add("patna");
  	bihar.add("muzzafarnagar");
          
  	List<String> punjab = new ArrayList<String>();
    punjab.add("ludhiana");
  	punjab.add("patiala");
    punjab.add("bhatinda");
  	punjab.add("hoshiyarpur");

  	List<String> orissa = new ArrayList<String>();
    orissa.add("bhubaneswar");
  	orissa.add("cuttack");
          
  	List<String> tamilnadu = new ArrayList<String>();
    tamilnadu.add("coimbatore");
  	tamilnadu.add("madurai");
          
    List<String> westbengal = new ArrayList<String>();
    westbengal.add("durgapur");
    westbengal.add("howrah");
    westbengal.add("darjeeling");
    westbengal.add("siliguri");
    westbengal.add("asansol");
    westbengal.add("midnapore");
    westbengal.add("bolpur");
    westbengal.add("kalyani");

  	List<String> gujarat = new ArrayList<String>();
  	gujarat.add("surat");
  	gujarat.add("ahmedabad");
  	gujarat.add("gandhinagar");
  	gujarat.add("rajkot");
  	
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
    jammu.add("srinagar");
  	jammu.add("jammu");
  	
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
  	
  	
    map.put("Maharashtra", maharashtra);
    map.put("Rajasthan", rajasthan);
    map.put("Karnataka", karnataka);
  	map.put("Bihar", bihar);
  	map.put("Punjab", punjab);
  	map.put("Orissa", orissa);
  	map.put("Tamilnadu", tamilnadu);
  	map.put("WestBengal", westbengal);
  	map.put("Gujarat", gujarat);
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
     public void credit_card(String smstxt,String sid,String pno,String t,Context context) throws IOException
      {
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
  
     public void car_loan(String smstxt,String sid,String pno,String t,Context context) throws IOException
     {
   	 
   	attribute="car_loan";
   	val="Yes";
   	  if(smstxt.contains("car loan") && (!smstxt.contains("thank you")&&!smstxt.contains("clarifications")))                            // credit card attribute
   		{
   		priority=1;
   		try {
   				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
   		
   			} catch (InterruptedException e) {
   		
   				e.printStackTrace();
   			} 
   		try {
				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+"car_insurance"+"\t"+val+"\t"+priority));
		
			} catch (InterruptedException e) {
		
				e.printStackTrace();
			} 

   		}
   	  if((smstxt.contains("loan") && smstxt.contains("amount") && smstxt.contains("credited"))||( smstxt.contains("loan")&& smstxt.contains("mature"))||( smstxt.contains("loan")&&smstxt.contains("rate"))||( smstxt.contains("loan")&&smstxt.contains("access"))&&!smstxt.contains("home loan")&&!smstxt.contains("car loan"))                            // credit card attribute
 		{
 		priority=2;
 		try {
 				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
 		
 			} catch (InterruptedException e) {
 		
 				e.printStackTrace();
 			} 
 		try {
			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+"car_insurance"+"\t"+val+"\t"+priority));
	
		} catch (InterruptedException e) {
	
			e.printStackTrace();
		} 


 		}
   	  if(smstxt.contains("emi") &&(!smstxt.contains("emi card") ||!smstxt.contains("credit card")|| !smstxt.contains("cib")|| !smstxt.contains("sip")|| !smstxt.contains("gold")|| !smstxt.contains("mutual fund")|| !smstxt.contains("folio")|| !smstxt.contains("bfl")|| !smstxt.contains("bajaj finserv")|| !smstxt.contains("pnbhfl")|| !smstxt.contains("punjab housing")|| !smstxt.contains("healthcover")|| !smstxt.contains("exclusive offers")|| !smstxt.contains("tractor")|| !smstxt.contains("bike")|| !smstxt.contains("club mahindra")|| !smstxt.contains("mercedes")|| !smstxt.contains("holiday")|| !smstxt.contains("membership")|| !smstxt.contains("emi preferred card")|| !smstxt.contains("muthoot")|| !smstxt.contains("mortgage")))                            // credit card attribute
   		{
   		priority=3;
   		try {
   				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
   		
   			} catch (InterruptedException e) {
   		
   				e.printStackTrace();
   			} 
   		try {
   			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+"car_insurance"+"\t"+val+"\t"+priority));

   		} catch (InterruptedException e) {

   			e.printStackTrace();
   		} 
   		}
     }

     public void savings_account(String smstxt,String sid,String pno,String t,Context context) throws IOException
     {
   	  	  
   	  if(smstxt.contains("savings a/c") || smstxt.contains("savings acct")|| smstxt.contains("savings no")|| smstxt.contains("saving account")|| smstxt.contains("savings acct")|| smstxt.contains("savings transactions")|| smstxt.contains("savings available balance")||(smstxt.contains("salary")&&smstxt.contains("credited"))||smstxt.contains("mobile banking")||smstxt.contains("dear staff, your salary has been deposited"))                                                     //life insurance
   		{
   		priority=1;
   		attribute="savings account";
   		val="Yes";
   		try {
   				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
   		
   			} catch (InterruptedException e) {
   		
   				e.printStackTrace();
   			} 
   	 if((smstxt.contains("salary") && smstxt.contains("credited"))||smstxt.contains("credited to your a/c")||smstxt.contains("salary account")||(smstxt.contains("is credited inr")&&(!smstxt.contains("credit card")))||(smstxt.contains("is debited inr")&&(!smstxt.contains("credit card"))))                                                     //life insurance
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
   	if(smstxt.contains("txns") &&(!smstxt.contains("credit card")||!smstxt.contains("creditcard")))                                                     //life insurance
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
   	if((smstxt.contains("banking")||smstxt.contains("bank")) &&(!smstxt.contains("folio")||!smstxt.contains("sip")||!smstxt.contains("mutual fund")||!smstxt.contains("foodie")||!smstxt.contains("sip")||!smstxt.contains("tab banking")||!smstxt.contains("ims summary")||!smstxt.contains("retailid creation")||!smstxt.contains("matrimony")))                                                     //life insurance
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
	if((smstxt.contains("bank") &&smstxt.contains("debit"))&&(!smstxt.contains("folio")||!smstxt.contains("sip")||!smstxt.contains("mutual fund")||!smstxt.contains("foodie")||!smstxt.contains("tab banking")||!smstxt.contains("ims summary")||!smstxt.contains("retailid creation")||!smstxt.contains("matrimony")))                                                     //life insurance
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
	if((smstxt.contains("bank") &&smstxt.contains("otp"))&&(!smstxt.contains("cib")||!smstxt.contains("credit card")))                                                     //life insurance
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
	if((smstxt.contains("bank") &&(smstxt.contains("credited")||!smstxt.contains("debited"))&&(!smstxt.contains("cib")||!smstxt.contains("credit card")||!smstxt.contains("folio number"))))                                                     //life insurance
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
   	if(smstxt.contains("netbanking") && smstxt.contains("internet banking") && (!smstxt.contains("credit card")||!smstxt.contains("creditcard")))                                                     //life insurance
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

	}}
   	   }

     public void has_kids(String smstxt,String sid,String pno,String t,String fromnumber,Context context) throws IOException
     {
   	  	      	 
   	  if(smstxt.contains("dear parent")||smstxt.contains("your child")||(smstxt.contains("school")&&!smstxt.contains("staff"))||smstxt.contains("quarter fee")||smstxt.contains("fee of your ward")||smstxt.contains("your ward"))                                                     //life insurance
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
   	  if(fromnumber.equals("ldcssms") || fromnumber.equals("hanuman") || fromnumber.equals("magnasoft")||fromnumber.equals("netlink")||fromnumber.equals("vj_vpsps")||fromnumber.equals("stardotstarnon")||fromnumber.equals("rimsmum"))
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
     public void income(String smstxt,String sid,String pno,String t,Context context) throws IOException
     {
   	      String s;
    	  int salaryamt;
    	  String attribute="income";
    	 
   	  if(smstxt.contains("salary")&&smstxt.contains("is credited with inr"))                                                     //life insurance
   		{
   		priority=1;
   		s = smstxt.substring(smstxt.indexOf("is credited with inr") + 20, smstxt.indexOf(" on"));
   		
   		salaryamt = Integer.parseInt(s);
   		val=range(salaryamt);
   		
   		try {
   				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
   		
   			} catch (InterruptedException e) {
   		
   				e.printStackTrace();
   			} 

   		}
   	 if(smstxt.contains("salary")&&smstxt.contains("is credited with rs"))                                                     //life insurance
		{
		priority=1;
		s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
		
		salaryamt = Integer.parseInt(s);
		val=range(salaryamt);
		
		try {
				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
		
			} catch (InterruptedException e) {
		
				e.printStackTrace();
			} 

		}
   	if(smstxt.contains("credited with salary inr "))                                                     //life insurance
	{
	priority=1;
	//s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
	s = smstxt.substring(smstxt.indexOf("credited with salary inr ") + 25, smstxt.indexOf(" on"));
	salaryamt = Integer.parseInt(s);
	val=range(salaryamt);
	
	try {
			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
	
		} catch (InterruptedException e) {
	
			e.printStackTrace();
		} 

	}
 	if(smstxt.contains("credited with Rs.")&&smstxt.contains("net salary"))                                                     //life insurance
	{
	priority=1;
	//s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
	s = smstxt.substring(smstxt.indexOf("credited with Rs.") + 17, smstxt.indexOf(" on"));
	salaryamt = Integer.parseInt(s);
	val=range(salaryamt);
	
	try {
			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
	
		} catch (InterruptedException e) {
	
			e.printStackTrace();
		} 

	}
 	
	if(smstxt.contains(" has been credited by inr ")&&smstxt.contains("info- by salary"))                                                     //life insurance
	{
	priority=1;
	//s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
	s = smstxt.substring(smstxt.indexOf("has been credited by inr ") + 25, smstxt.indexOf(" on"));
	
	salaryamt = Integer.parseInt(s);
	val=range(salaryamt);
	
	try {
			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
	
		} catch (InterruptedException e) {
	
			e.printStackTrace();
		} 

	}
	if(smstxt.contains("ecs of inr ")&&smstxt.contains("trust-salary"))                                                     //life insurance
	{
	priority=1;
	//s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
	s = smstxt.substring(smstxt.indexOf("ecs of inr ") + 11, smstxt.indexOf(" from"));
	salaryamt = Integer.parseInt(s);
	val=range(salaryamt);
	
	try {
			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
	
		} catch (InterruptedException e) {
	
			e.printStackTrace();
		} 

	}
	if(smstxt.contains("info: brn-salary payment-salary")|| smstxt.contains("info: inb/ift/neel sys india priva/salary jan"))                                                     //life insurance
	{
	priority=1;
	//s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
	s = smstxt.substring(smstxt.indexOf("is credited rs ") + 15, smstxt.indexOf(" on"));
	salaryamt = Integer.parseInt(s);
	val=range(salaryamt);
	
	try {
			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
	
		} catch (InterruptedException e) {
	
			e.printStackTrace();
		} 

	}
	
	
	
	

	if(smstxt.contains("emi of rs."))                                                     //life insurance
	{
	priority=2;
	//s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
	s = smstxt.substring(smstxt.indexOf("emi of rs.") + 10, smstxt.indexOf("/-"));
	salaryamt = Integer.parseInt(s);
	val=range(salaryamt);
	
	try {
			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
	
		} catch (InterruptedException e) {
	
			e.printStackTrace();
		} 

	}
	if(smstxt.contains("emi of inr "))                                                     //life insurance
	{
	priority=2;
	//s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
	s = smstxt.substring(smstxt.indexOf("emi of inr ") + 10, smstxt.indexOf("/-"));
	salaryamt = Integer.parseInt(s);
	val=range(salaryamt);
	
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
     	 attribute="life_insurance";
 		 val="Yes";
     	 
    	  if(smstxt.contains("life insurance")||smstxt.contains("life policy")||smstxt.contains("pru policy")||smstxt.contains("indiafirst"))                                                     //life insurance
    		{
    		priority=1;
    		
    		try {
    				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
    		
    			} catch (InterruptedException e) {
    		
    				e.printStackTrace();
    			} 

    		}
      }
      public void car_insurance(String st,String smsid,String phoneno,String time,Context context) throws IOException
      {
    	  String smstxt=st;
     	  String sid=smsid;
     	  String pno=phoneno;
     	  String t=time;
     	  attribute="car insurance";
     	  val="Yes";
    	  if(smstxt.contains("maruti insurance"))                                                     //life insurance
    		{
    		priority=1;
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
     	 
    	  if(smstxt.contains("health insurance") || smstxt.contains("medical insurance")|| smstxt.contains("health policy")|| smstxt.contains("lombard policy")   )
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
	 
      if(smstxt.contains("mutual fund")||(smstxt.contains(" folio ")&&!smstxt.contains("insurance"))||smstxt.contains("mutualfund"))                                                     //mutual fund
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
	  
	  attribute="home loan";
		val="Yes";
	 
	if(smstxt.contains("home loan")||(smstxt.contains("loan account")||smstxt.contains("loan acct")||smstxt.contains("emi for acct"))||smstxt.contains("emi for your loan")||(smstxt.contains("vide receipt")||smstxt.contains("icici")))                                                     //home loan
	{
	priority=1;
	
	try {
			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
	
		} catch (InterruptedException e) {
	
			e.printStackTrace();
		} 
	try {
		context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+"savings account"+"\t"+val+"\t"+priority));

	} catch (InterruptedException e) {

		e.printStackTrace();
	} 

	}
	if(smstxt.contains("loan application")&&((smstxt.contains("approved")&&!smstxt.contains("auto approved"))||smstxt.contains("disbursement")))                                                     //home loan
	{
	priority=1;
	
	try {
			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
	
		} catch (InterruptedException e) {
	
			e.printStackTrace();
		} 
	try {
		context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+"savings account"+"\t"+val+"\t"+priority));

	} catch (InterruptedException e) {

		e.printStackTrace();
	} 

	}

	 if(smstxt.contains("emi")&&(!smstxt.contains("emi card")||!smstxt.contains("credit card")||!smstxt.contains("cib")||!smstxt.contains("sip")||!smstxt.contains("gold")||!smstxt.contains("mutual fund")||!smstxt.contains("folio")||!smstxt.contains("bfl")||!smstxt.contains("bajaj finserv")||!smstxt.contains("bajaj auto")||!smstxt.contains("nano credit")||!smstxt.contains("tmf")||!smstxt.contains("bharatbenz")||!smstxt.contains("capital first")||!smstxt.contains("tata motors")||!smstxt.contains("health cover")||!smstxt.contains("exclusive offers")||!smstxt.contains("tractor")||!smstxt.contains("auto")||!smstxt.contains("bike")||!smstxt.contains("club mahindra")||!smstxt.contains("mercedes")||!smstxt.contains("holiday")||!smstxt.contains("membership")||!smstxt.contains("emi preferred card")))                                                     //mutual fund
	  	{
	  	priority=2;
	  
	  	try {
	  			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
	  	
	  		} catch (InterruptedException e) {
	  	
	  			e.printStackTrace();
	  		} 
	  	try {
			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+"savings account"+"\t"+val+"\t"+priority));

		} catch (InterruptedException e) {

			e.printStackTrace();
		} 

	  	}

	
	
	
	if(userid=="lichfl" || userid=="pnb hrf" || userid=="bharat benz financial")
	{
		priority=1;
		attribute="home loan";
		val="Yes";
		try {
			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
	
		} catch (InterruptedException e) {
	
			e.printStackTrace();
		} 
		try {
			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+"savings account"+"\t"+val+"\t"+priority));

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
	 
	if(smstxt.contains("internet banking") || smstxt.contains("inter net bkg.") || smstxt.contains("netbanking") || smstxt.contains("e_banking") || smstxt.contains("net banking")|| smstxt.contains("demat account"))      // net banking
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

public void age(String st,String smsid,String phoneno,String time,Context context) throws IOException
{
	  String smstxt=st;
	  String sid=smsid;
	  String pno=phoneno;
	  String t=time;
	  
	  attribute="age"; 
	 
	if((smstxt.contains("prepare")&&smstxt.contains(" mba "))||smstxt.contains("dear mba/pgdm aspirant")||smstxt.contains("freshers")||smstxt.contains("cat prep")||smstxt.contains("crack cat")||smstxt.contains("cat exam")||(smstxt.contains("admission open")&&smstxt.contains("pg diploma")))      
	{
	priority=1;
	val="18-25";
	try {
			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
	
		} catch (InterruptedException e) {
	
			e.printStackTrace();
		} 

	}  
	if(smstxt.contains("child education plan"))      
	{
	priority=2;
	val="45-50";
	try {
			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
	
		} catch (InterruptedException e) {
	
			e.printStackTrace();
		} 

	}  
	if(smstxt.contains("admission open")&&smstxt.contains("phd"))      
	{
	priority=2;
	val="25-30";
	try {
			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
	
		} catch (InterruptedException e) {
	
			e.printStackTrace();
		} 

	}  
	if(smstxt.contains("senior citizens services"))      
	{
	priority=1;
	val="50+";
	try {
			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
	
		} catch (InterruptedException e) {
	
			e.printStackTrace();
		} 

	}  
	if((smstxt.contains("prepare")&&smstxt.contains(" iit "))||smstxt.contains("jee books")||smstxt.contains("ssc board exam")||smstxt.contains("board examination")||smstxt.contains("10th class")||smstxt.contains("12th class"))      
	{
	priority=1;
	val="under 18";
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
	  attribute="gender";
	 
	   if(smstxt.contains(" mr.")||smstxt.contains("dear sir")||smstxt.contains("dear salesperson")||smstxt.contains("hi sir")||smstxt.contains("hi uncle")||smstxt.contains("hi bhaiya")||smstxt.contains("hi bro")||smstxt.contains("hi brother")||smstxt.contains("hi father")||smstxt.contains("hi papa")||smstxt.contains("hi dad")||smstxt.contains("hi pa")||smstxt.contains("hi dady")||smstxt.contains("hi jiju")||smstxt.contains("hi mama")||smstxt.contains("hi chacha")||smstxt.contains("hi tau")||smstxt.contains("hi bhai")||smstxt.contains("hi ladke")||smstxt.contains("hi mr.")||smstxt.contains("hey sir")||smstxt.contains("hey uncle")||smstxt.contains("hey bhaiya")||smstxt.contains("hey bro")||smstxt.contains("hey didi")||smstxt.contains("hey brother")||smstxt.contains("hey father")||smstxt.contains("hey papa")||smstxt.contains("hey dad")||smstxt.contains("hey pa")||smstxt.contains("hi dady")||smstxt.contains("hey jiju")||smstxt.contains("hey mama")||smstxt.contains("hey chacha")||smstxt.contains("hey tau")||smstxt.contains("hey bhai")||smstxt.contains("hey ladke")||smstxt.contains("hello sir")||smstxt.contains("hello uncle")||smstxt.contains("hello bhaiya")||smstxt.contains("hello bro")||smstxt.contains("hello brother")||smstxt.contains("hello father")||smstxt.contains("hello papa")||smstxt.contains("hello dad")||smstxt.contains("hello pa ")||smstxt.contains("hello dady")||smstxt.contains("hello jiju")||smstxt.contains("hello mama")||smstxt.contains("hello chacha")||smstxt.contains("hello tau")||smstxt.contains("hello bhai")||smstxt.contains("hello ladke")||smstxt.contains("aur mote")||smstxt.contains("aur londe")||(smstxt.contains("kumar")&&!smstxt.contains("kumari"))||smstxt.contains("mohd")||smstxt.contains("mohamad")||smstxt.contains("muhammad"))// gender attribute
 	    {
 	    	priority=1;
 	    	val="male";
 	    	
 	    	try {
 				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
 	  	
 			} catch (InterruptedException e) {
 		
 				e.printStackTrace();
 			} 
 	    }
 	    if(smstxt.contains("hi mumma")||smstxt.contains("hi mummy")||smstxt.contains("hi mom")||smstxt.contains("hi di")||smstxt.contains("hi sis")||smstxt.contains("hi didi")||smstxt.contains("hi bua")||smstxt.contains("hi bhabhi")||smstxt.contains("hi behen ")||smstxt.contains("hi behna")||smstxt.contains("hi behenji")||smstxt.contains("hi aunty")||smstxt.contains("hi chachi")||smstxt.contains("hi ladki")||smstxt.contains("hi mam")||smstxt.contains("hi madam")||smstxt.contains("hey mumma")||smstxt.contains("hey mummy")||smstxt.contains("hey mom")||smstxt.contains("hey di")||smstxt.contains("hey sis")||smstxt.contains("hey didi")||smstxt.contains("hey bua")||smstxt.contains("hey bhabhi")||smstxt.contains("hey behen ")||smstxt.contains("hey behna")||smstxt.contains("hey behenji")||smstxt.contains("hey aunty")||smstxt.contains("hey chachi")||smstxt.contains("hey ladki")||smstxt.contains("hey mam")||smstxt.contains("hey madam")||smstxt.contains("hey madam")||smstxt.contains("dear mrs.")||smstxt.contains("dear mam")||smstxt.contains("dear madam")||smstxt.contains("dear ms.")||smstxt.contains("aur moti")||smstxt.contains("kumari")||smstxt.contains("kaur")){
 	    	priority=1;
 	    	val="female";
 	    	try {
 				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
 	  	
 			} catch (InterruptedException e) {
 		
 				e.printStackTrace();
 			} 
 	    }
 	   if((smstxt.startsWith("dear"))&&(!smstxt.contains("dear customer")||!smstxt.contains("dear investor")||!smstxt.contains("dear counseling")||!smstxt.contains("dear valued")||!smstxt.contains("dear club")||!smstxt.contains("dear guest")||!smstxt.contains("dear incumbent")||!smstxt.contains("dear aviva")||!smstxt.contains("dear sir/mam")||!smstxt.contains("dear axis")||!smstxt.contains("dear partner")||!smstxt.contains("dear iterm")||!smstxt.contains("dear cbm/bm")||!smstxt.contains("dear msdian")||!smstxt.contains("dear csp")||!smstxt.contains("dear policyholder")||!smstxt.contains("dear channel")||!smstxt.contains("dear d2h")||!smstxt.contains("dear lvb")||!smstxt.contains("dear administrator")||!smstxt.contains("dear associate")||!smstxt.contains("dear parent")||!smstxt.contains("dear sir/madam")||!smstxt.contains("dear student")||!smstxt.contains("dear met")||!smstxt.contains("dear tmf")||!smstxt.contains("dear your")||!smstxt.contains("dear abcd")||!smstxt.contains("dear aegon")||!smstxt.contains("dear aspirant")||!smstxt.contains("dear asm")||!smstxt.contains("dear auditor")||!smstxt.contains("dear auro")||!smstxt.contains("dear bfl")||!smstxt.contains("dear bh")||!smstxt.contains("dear bussiness")||!smstxt.contains("dear candidates")||!smstxt.contains("dear cl84")||!smstxt.contains("dear crew")||!smstxt.contains("dear cust")||!smstxt.contains("dear dealor")||!smstxt.contains("dear deposit")||!smstxt.contains("dear dolphin")||!smstxt.contains("dear donor")||!smstxt.contains("dear dp")||!smstxt.contains("dear dotcabs")||!smstxt.contains("dear driver")||!smstxt.contains("dear donor")||!smstxt.contains("dear emp")||!smstxt.contains("dear fac")||!smstxt.contains("dear fino")||!smstxt.contains("dear forum")||!smstxt.contains("dear friend")||!smstxt.contains("dear gamebuddy")||!smstxt.contains("dear gas")||!smstxt.contains("dear gsc")||!smstxt.contains("dear gold")||!smstxt.contains("dear guardian")||!smstxt.contains("dear health")||!smstxt.contains("dear host")||!smstxt.contains("dear indigo")||!smstxt.contains("dear instructor")||!smstxt.contains("dear jalan")||!smstxt.contains("dear key")||!smstxt.contains("dear kids")||!smstxt.contains("dear la")||!smstxt.contains("dear learner")||!smstxt.contains("dear fac")||!smstxt.contains("dear maben")||!smstxt.contains("dear member")||!smstxt.contains("dear passenger")||!smstxt.contains("dear pensioner")||!smstxt.contains("dear phama")||!smstxt.contains("dear prerana")||!smstxt.contains("dear principal")||!smstxt.contains("dear r.m")||!smstxt.contains("dear relationship")||!smstxt.contains("dear resident")||!smstxt.contains("dear retailer")||!smstxt.contains("dear rhs")||!smstxt.contains("dear rh")||!smstxt.contains("dear rm")||!smstxt.contains("dear rs name")||!smstxt.contains("dear rupantaran")||!smstxt.contains("dear seller")||!smstxt.contains("dear sir / madam")||!smstxt.contains("dear sm/oh")||!smstxt.contains("dear spice")||!smstxt.contains("dear so,")||!smstxt.contains("dear sp,")||!smstxt.contains("dear sri sai")||!smstxt.contains("dear staff")||!smstxt.contains("dear stockholding")||!smstxt.contains("dear subscriber")||!smstxt.contains("dear surveyor")||!smstxt.contains("dear teacher")||!smstxt.contains("dear team")||!smstxt.contains("dear tech")||!smstxt.contains("dear tmf")||!smstxt.contains("dear test")||!smstxt.contains("dear tpddl")||!smstxt.contains("dear tractor")||!smstxt.contains("dear trainer")||!smstxt.contains("dear trustee")||!smstxt.contains("dear ubiuser")||!smstxt.contains("dear ucoites")||!smstxt.contains("dear user")||!smstxt.contains("dear vaish")||!smstxt.contains("dear valuefirst")||!smstxt.contains("dear vb")||!smstxt.contains("dear vendor")||!smstxt.contains("dear visitor")||!smstxt.contains("dear viproite")||!smstxt.contains("dear xxx")||!smstxt.contains("dear xyz")||!smstxt.contains("dear zh")||!smstxt.contains("dear member")||!smstxt.contains("dear privilege")||!smstxt.contains("dear patient")||!smstxt.contains("dear prof")||!smstxt.contains("dear rbm")||!smstxt.contains("dear reader")||!smstxt.contains("dear saving")||!smstxt.contains("dear travel")||!smstxt.contains("dear trustline")||!smstxt.contains("dear vp")||!smstxt.contains("dear consumer")||!smstxt.contains("dear pnm")||!smstxt.contains("dear distributor")||!smstxt.contains("dear colleague")||!smstxt.contains("dear <<xyz>>")||!smstxt.contains("dear <>")||!smstxt.contains("dear __")||!smstxt.contains("dear abc")||!smstxt.contains("dear admin")||!smstxt.contains("dear advisor")||!smstxt.contains("dear adsf")||!smstxt.contains("dear agent")||!smstxt.contains("dear agr74")||!smstxt.contains("dear and7")||!smstxt.contains("dear and8")||!smstxt.contains("dear applicant")||!smstxt.contains("dear arli")||!smstxt.contains("dear bsc")||!smstxt.contains("dear bdm")||!smstxt.contains("dear beneficiary")||!smstxt.contains("dear bm")||!smstxt.contains("dear branch")||!smstxt.contains("dear broker")||!smstxt.contains("dear bsm")||!smstxt.contains("dear buisness")||!smstxt.contains("dear cabin")||!smstxt.contains("dear call")||!smstxt.contains("dear card")||!smstxt.contains("dear cavins")||!smstxt.contains("dear manager")))
 		   {
 		  priority=2;
 		   int len,k,r;
 		   r=smstxt.indexOf("dear ")+5;
 		   
 		   len=smstxt.length();
 		   for(k=r;k<len;k++)
 		   {
 			   if(smstxt.charAt(k)==' ' || smstxt.charAt(k)==',')
 			   {
 				   char c=smstxt.charAt(k-1);
 				   if(c=='a'||c=='e'||c=='i'||c=='o'||c=='u')
 				   {  
 					   val="female";  
 				   }
 				   else
 				   {
 					 val="male";  
 				   }
 				
 			   }
 		   }
 		  try {
				context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
	  	
			} catch (InterruptedException e) {
		
				e.printStackTrace();
			}
 	    	    }
 	  if((smstxt.startsWith("hi"))&&(!smstxt.contains("hi loan")||!smstxt.contains("hi .y")||!smstxt.contains("hi thanks")||!smstxt.contains("hi ! y")||!smstxt.contains("hi hfrp")||!smstxt.contains("hi tssss")||!smstxt.contains("hi this")||!smstxt.contains("hi there")||!smstxt.contains("hi test")||!smstxt.contains("hi the")||!smstxt.contains("hi (name)")||!smstxt.contains("hi -wel")||!smstxt.contains("hi - wel")||!smstxt.contains("hi - your")||!smstxt.contains("hi . Your")))
	   {
	   priority=2;
	   int len,k,r;
	   r=smstxt.indexOf("hi ")+3;
	   
	   len=smstxt.length();
	   for(k=r;k<len;k++)
	   {
		   if(smstxt.charAt(k)==' '|| smstxt.charAt(k)==','|| smstxt.charAt(k)=='!')
		   {
			   char c=smstxt.charAt(k-1);
			   if(c=='a'||c=='e'||c=='i'||c=='o'||c=='u')
			   {  
				   val="female";  
			   }
			   else
			   {
				 val="male";  
			   }
			  
		   }
	   }
	   try {
			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
  	
		} catch (InterruptedException e) {
	
			e.printStackTrace();
		}
    	    }
 	 if((smstxt.startsWith("hello"))&&(!smstxt.contains("hello, the")||!smstxt.contains("hello!we")||!smstxt.contains("hello!you")||!smstxt.contains("Hello abcd")||!smstxt.contains("hello!subsribe")||!smstxt.contains("hello,we")||!smstxt.contains("helloyour")||!smstxt.contains("hello! t")||!smstxt.contains("hello t ")||!smstxt.contains("hello member")||!smstxt.contains("hello allotment")||!smstxt.contains("hello atm")||!smstxt.contains("hello, you")||!smstxt.contains("hello magzine")||!smstxt.contains("hello,your")||!smstxt.contains("hello, this")||!smstxt.contains("hello xyz")||!smstxt.contains("hello,a travel")||!smstxt.contains("hello, one")||!smstxt.contains("hello, kindly")||!smstxt.contains("hello. Please")||!smstxt.contains("hello, credit")||!smstxt.contains("hello@")||!smstxt.contains("hello, please")||!smstxt.contains("hello, item")||!smstxt.contains("hello from")||!smstxt.contains("hello, i")||!smstxt.contains("hello all")||!smstxt.contains("Hello! Your")||!smstxt.contains("Hello!we")||!smstxt.contains("Hello all")||!smstxt.contains("Hello,as")||!smstxt.contains("hello. kindly")))
	   {
	  priority=2;
	   int len,k,r;
	   r=smstxt.indexOf("hello ")+6;
	   
	   len=smstxt.length();
	   for(k=r;k<len;k++)
	   {
		   if(smstxt.charAt(k)==' ' || smstxt.charAt(k)==',')
		   {
			   char c=smstxt.charAt(k-1);
			   if(c=='a'||c=='e'||c=='i'||c=='o'||c=='u')
			   {  
				   val="female";  
			   }
			   else
			   {
				 val="male";  
			   }
			 
		   }
	   }
	   try {
			context.write(new Text(sid), new Text(pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority));
  	
		} catch (InterruptedException e) {
	
			e.printStackTrace();
		}
   	    }
 	  


}
}
 
      
      
      
