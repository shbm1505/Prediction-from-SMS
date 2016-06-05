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

public class WCMapper extends Mapper < LongWritable, Text, Text, Text > {
  String attributeValue = " ";
  String attributeName = " ";
  int priority=0;
  public void map(LongWritable key, Text value, Context context) throws IOException {
   String line;
   String smsText;
   String smsId;
   String time;
   String phoneNo;
   String userid;
   String fromNumber;
   String circle;
   String sms[] = new String[20];
   

   String smsLower;
   // String substring[] = new String[20];
   //todo need to crossverify it.
   WCMapper objMapper = new WCMapper();

   line = value.toString();
   try {
    sms = line.split("\\#\\|");

    smsText = sms[13];
    smsId = sms[0];
    time = sms[10];
    phoneNo = sms[3];
    userid = sms[2];
    fromNumber = sms[4];
    circle = sms[9];

    smsLower = smsText.toLowerCase() + " ";
    //  substring = smsLower.split(" ");

    objMapper.location(smsLower, smsId, phoneNo, time, context, circle);
    objMapper.credit_card(smsLower, smsId, phoneNo, time, context);
    objMapper.life_insurance(smsLower, smsId, phoneNo, time, fromNumber, context);
    objMapper.health_insurance(smsLower, smsId, phoneNo, time, fromNumber, context);
    objMapper.mutual_fund(smsLower, smsId, phoneNo, time, context);
    objMapper.home_loan(smsLower, smsId, phoneNo, time, fromNumber, context);
    objMapper.internet_banking(smsLower, smsId, phoneNo, time, context);
    objMapper.gender(smsLower, smsId, phoneNo, time, context);
    objMapper.has_kids(smsLower, smsId, phoneNo, time, fromNumber, context);
    objMapper.savings_account(smsLower, smsId, phoneNo, time, context);
    objMapper.car_loan(smsLower, smsId, phoneNo, time, fromNumber, context);
    objMapper.income(smsLower, smsId, phoneNo, time, fromNumber, context);
    objMapper.age(smsLower, smsId, phoneNo, time, context);
    objMapper.apparel(smsLower, smsId, phoneNo, time, context);
    objMapper.gadgets(smsLower, smsId, phoneNo, time, context);
    objMapper.sports(smsLower, smsId, phoneNo, time, context);
    objMapper.movies(smsLower, smsId, phoneNo, time, fromNumber, context);
    objMapper.frequent_traveller(smsLower, smsId, phoneNo, time, fromNumber, context);
    objMapper.frequent_shopper(smsLower, smsId, phoneNo, time, fromNumber, context);
    objMapper.car_insurance(smsLower, smsId, phoneNo, time, fromNumber, context);
   } catch (Exception ex) {
    //ex.printStackTrace();
   }
  }

  //todo need to review and add more rules
  public void location(String smstxt, String smsId, String phoneNo, String time, Context context, String circle) throws IOException {
   Map < String, List < String >> map = new HashMap < String, List < String >> ();

   List < String > maharashtra = new ArrayList < String > ();
   maharashtra.add("pune");
   maharashtra.add("nagpur");
   maharashtra.add("ahmednagar");
   maharashtra.add("akola");
   maharashtra.add("amravati");
   maharashtra.add("aurangabad");
   maharashtra.add("beed");
   maharashtra.add("bhandara");
   maharashtra.add("buldana");
   maharashtra.add("palghar");
   maharashtra.add("wardha");
   maharashtra.add("solapur");
   maharashtra.add("satara");
   maharashtra.add("nashik");
   maharashtra.add("latur");
   maharashtra.add("kolhapur");
   maharashtra.add("jalna");
   maharashtra.add("jalgaon");
   maharashtra.add("hingoli");
   maharashtra.add("gondia");
   maharashtra.add("gadchiroli");
   maharashtra.add("dhule");
   maharashtra.add("chandrapur");
  


   List < String > rajasthan = new ArrayList < String > ();
   rajasthan.add("udaipur");
   rajasthan.add("jodhpur");
   rajasthan.add("jaisalmer");
   rajasthan.add("jaipur");
   rajasthan.add("ajmer");
   rajasthan.add("bikaner");
   rajasthan.add("kota");
   rajasthan.add("alwar");
   rajasthan.add("bharatpur");
   rajasthan.add("dholpur");
   rajasthan.add("rajsamand");
   rajasthan.add("pali");
   rajasthan.add("sikar");
   rajasthan.add("sirohi");
   
   List < String > haryana = new ArrayList < String > ();
   haryana.add("ambala");
   haryana.add("bhiwani");
   haryana.add("fatehabad");
   haryana.add("hisar");
   haryana.add("jhajjar");
   haryana.add("jind");
   haryana.add("kaithal");
   haryana.add("karnal");
   haryana.add("kurukshetra");
   haryana.add("mahendragarh");
   haryana.add("palwal");
   haryana.add("panipat");
   haryana.add("rohtak");
   haryana.add("sonipat");
   
     
   List < String > himachal = new ArrayList < String > ();
   himachal.add("bilaspur");
   himachal.add("chamba");
   himachal.add("hamirpur");
   himachal.add("kangra");
   himachal.add("kinnaur");
   himachal.add("kullu");
   himachal.add("mandi");
   himachal.add("shimla");
   himachal.add("solan");
   himachal.add("unna");
   
   List < String > karnataka = new ArrayList < String > ();
   karnataka.add("bangalore");
   karnataka.add("mysore");
   karnataka.add("ooty");
   karnataka.add("bangaluru");
   karnataka.add("bangalkot");
   karnataka.add("bellary");
   karnataka.add("vijayapura");
   karnataka.add("dharwad");
   karnataka.add("gadag");
   karnataka.add("kalaburagi");
   karnataka.add("hassan");
   karnataka.add("haveri");
   karnataka.add("kodagu");
   karnataka.add("kolar");
   karnataka.add("koppal");
   karnataka.add("mandya");
   karnataka.add("mysuru");
   karnataka.add("raichur");
   

   List < String > bihar = new ArrayList < String > ();
   bihar.add("patna");
   bihar.add("muzzafarnagar");
   bihar.add("ranchi");
   bihar.add("khunti");
   bihar.add("chatra");
   bihar.add("samastipur");
   bihar.add("saharsa");
   bihar.add("rohtas");
   bihar.add("purnia");
   bihar.add("nawada");
   bihar.add("nalanda");
   bihar.add("muzaffarpur");
   bihar.add("madhepura");
   bihar.add("munger");
   bihar.add("madhubani");
   bihar.add("lakhisarai");
   bihar.add("katihar");
   bihar.add("kaimur");
   bihar.add("kishanganj");
   bihar.add("khagaria");
   bihar.add("jehanabad");
   bihar.add("jamui");
   bihar.add("gopalganj");
   bihar.add("gaya");
   bihar.add("east champaran");
   bihar.add("darbhanga");
   bihar.add("buxar");
   bihar.add("bhojpur");
   bihar.add("bhagalpur");
   bihar.add("begusarai");
   bihar.add("banka");
   bihar.add("aurangabad");
   bihar.add("arwal");
   bihar.add("araria");
  
   List < String > northeast = new ArrayList < String > ();
   northeast.add("gomati");
   northeast.add("agartala");
   northeast.add("tuensang");
   northeast.add("kohima");
   northeast.add("dimapur");
   northeast.add("ampati");
   northeast.add("tura");
   northeast.add("jowai");
   northeast.add("namsai");
   northeast.add("tirap");
   northeast.add("tawang");
   northeast.add("lohit");
   northeast.add("anjaw");
   northeast.add("changlang");
  
  
      

   List < String > punjab = new ArrayList < String > ();
   punjab.add("ludhiana");
   punjab.add("patiala");
   punjab.add("bhatinda");
   punjab.add("hoshiyarpur");
   punjab.add("hoshiarpur");
   punjab.add("jalandhar");
   punjab.add("amritsar");
   punjab.add("barnala");
   punjab.add("bathinda");
   punjab.add("faridkot");
   punjab.add("firozpur");
   punjab.add("gurdaspur");
   punjab.add("ludhiana");
   punjab.add("kapurthala");
   punjab.add("mansa");
   punjab.add("moga");
   punjab.add("pathankot");
   punjab.add("chandigarh");
   punjab.add("panchkula");
   punjab.add("patiala");
   punjab.add("sangrur");
   
   
  
   List < String > orissa = new ArrayList < String > ();
   orissa.add("bhubaneswar");
   orissa.add("cuttack");
   orissa.add("anugul");
   orissa.add("baudh");
   orissa.add("balangir");
   orissa.add("bargarh");
   orissa.add("baleswar");
   orissa.add("bhadrak");
   orissa.add("deogarh");
   orissa.add("ganjam");
   orissa.add("jharsuguda");
   orissa.add("puri");
   orissa.add("nayagarh");
   orissa.add("nuapada");
   orissa.add("mayurbhanj");
   orissa.add("koraput");
   orissa.add("gajapati");
   

   
   

   List < String > tamilnadu = new ArrayList < String > ();
   tamilnadu.add("coimbatore");
   tamilnadu.add("madurai");
   tamilnadu.add("ariyalpur");
   tamilnadu.add("erode");
   tamilnadu.add("kanchipuram");
   tamilnadu.add("kanyakumari");
   tamilnadu.add("karur");
   tamilnadu.add("coimbatore");
   tamilnadu.add("nilgiris");
   tamilnadu.add("vellore");
   tamilnadu.add("salem");
   
   
   
   
   

   List < String > westbengal = new ArrayList < String > ();
   westbengal.add("durgapur");
   westbengal.add("howrah");
   westbengal.add("darjeeling");
   westbengal.add("siliguri");
   westbengal.add("asansol");
   westbengal.add("midnapore");
   westbengal.add("bolpur");
   westbengal.add("kalyani");
   westbengal.add("bankura");
   westbengal.add("howrah");
   westbengal.add("midnapore");
   westbengal.add("hooghly");
   
   
   
   

   List < String > gujarat = new ArrayList < String > ();
   gujarat.add("surat");
   gujarat.add("ahmedabad");
   gujarat.add("gandhinagar");
   gujarat.add("rajkot");
   gujarat.add("porbandar");
   gujarat.add("patan");
   gujarat.add("panchmahal");
   gujarat.add("navsari");
   gujarat.add("narmada");
   gujarat.add("morbi");
   gujarat.add("mehsana");
   gujarat.add("mahisagar");
   gujarat.add("kheda");
   gujarat.add("kutch");
   gujarat.add("dang");
   gujarat.add("dahod");
   gujarat.add("botad");
   gujarat.add("bhavnagar");
   gujarat.add("bharuch");
   gujarat.add("banaskantha");
   gujarat.add("aravali");
   gujarat.add("anand");
   gujarat.add("amreli");
     
   List < String > madhyapradesh = new ArrayList < String > ();
   madhyapradesh.add("bhopal");
   madhyapradesh.add("indore");
   madhyapradesh.add("jabalpur");
   madhyapradesh.add("ujjain");
   madhyapradesh.add("gwalior");
   madhyapradesh.add("ratlam");
   madhyapradesh.add("neemuch");
   madhyapradesh.add("chhatarpur");
   madhyapradesh.add("singrauli");
   madhyapradesh.add("sidhi");
   madhyapradesh.add("satna");
   madhyapradesh.add("rewa");
   madhyapradesh.add("hoshangabad");
   madhyapradesh.add("harda");
   madhyapradesh.add("seoni");
   madhyapradesh.add("dindori");
   madhyapradesh.add("mandla");
   madhyapradesh.add("katni");
   madhyapradesh.add("chhindwara");
   madhyapradesh.add("balaghat");
   madhyapradesh.add("jhabua");
   madhyapradesh.add("dhar");
   madhyapradesh.add("burhanpur");
   madhyapradesh.add("barwani");
   madhyapradesh.add("alirajpur");
   madhyapradesh.add("guna");
   madhyapradesh.add("datia");
   madhyapradesh.add("shivpuri");
   madhyapradesh.add("ashoknagar");
   madhyapradesh.add("sehore");
   madhyapradesh.add("rajgarh");
   
   
   
   
   
   

   List < String > kerala = new ArrayList < String > ();
   kerala.add("kochi");
   kerala.add("thiruvananthapuram");
   kerala.add("thrissur");
   kerala.add("wayanad");
   kerala.add("kollam");
   kerala.add("kannur");
   kerala.add("alappuzha");
   
   List < String > andhrapradesh = new ArrayList < String > ();
   andhrapradesh.add("hyderabad");
   andhrapradesh.add("vijayawada");
   andhrapradesh.add("secunderabad");
   andhrapradesh.add("vizianagaram");
   andhrapradesh.add("prakasam");
   andhrapradesh.add("west godavari");
   andhrapradesh.add("kurnool");
   andhrapradesh.add("srikakulam");
   andhrapradesh.add("ysr");
   andhrapradesh.add("visakhapatnam");
   andhrapradesh.add("krishna");
   
   List < String > jammu = new ArrayList < String > ();
   jammu.add("srinagar");
   jammu.add("jammu");
   jammu.add("doda");
   jammu.add("kishtwar");
   jammu.add("rajouri");
   jammu.add("reasi");
   jammu.add("udhampur");
   jammu.add("ramban");
   jammu.add("kathua");
   jammu.add("samba");
   jammu.add("poonch");
   jammu.add("anantnag");
   jammu.add("kulgam");
   jammu.add("kargil");
   jammu.add("leh");
   jammu.add("kupwara");
   jammu.add("ganderbal");
   

   List < String > assam = new ArrayList < String > ();
   assam.add("guwahati");
   assam.add("silchar");
   assam.add("jorhat");
   assam.add("tezpur");
   assam.add("dibrugarh");

   List < String > uttarpradesheast = new ArrayList < String > ();
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

   List < String > uttarpradeshwest = new ArrayList < String > ();
   uttarpradeshwest.add("meerut");
   uttarpradeshwest.add("bulandshahr");
   uttarpradeshwest.add("hapur");
   uttarpradeshwest.add("baghpat");
   uttarpradeshwest.add("saharanpur");
   uttarpradeshwest.add("shamli");
   uttarpradeshwest.add("moradabad");
   uttarpradeshwest.add("bijnor");
   uttarpradeshwest.add("rampur");
   uttarpradeshwest.add("barelly");
   uttarpradeshwest.add("pilibhit");
   uttarpradeshwest.add("shahjahanpur");
   uttarpradeshwest.add("amroha");
   uttarpradeshwest.add("hathras");
   uttarpradeshwest.add("kasganj");
   uttarpradeshwest.add("baghpat");
   uttarpradeshwest.add("dehradun");
   uttarpradeshwest.add("champawat");
   uttarpradeshwest.add("chamoli");
   uttarpradeshwest.add("bageshwar");
   uttarpradeshwest.add("almora");
   uttarpradeshwest.add("nainital");
   uttarpradeshwest.add("uttarkashi");
   uttarpradeshwest.add("haridwar");
   uttarpradeshwest.add("aligarh");
   uttarpradeshwest.add("mathura");


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
   map.put("NorthEast", northeast);
   map.put("HimachalPradesh", himachal);
   map.put("UttarPradeshWest", uttarpradeshwest);
   map.put("Haryana", haryana);
   
   attributeName = "location";
  // int priority;
if (circle.equals("Mumbai")) {
    priority = 1;
    attributeValue = "Mumbai";
   }
   if (circle.equals("Kolkata")) {
    priority = 1;
    attributeValue = "Kolkata";
   }
   if (circle.equals("Chennai")) {
    priority = 1;
    attributeValue = "Chennai";
   }
   for (Map.Entry < String, List < String >> entry: map.entrySet()) {
    String keyss = entry.getKey();
    if (circle.equals(keyss)) {
     List < String > values = entry.getValue();
     for (String city: values) {
      if (smstxt.contains(city) && !smstxt.contains(city + "road")) {
       priority = 2;
       attributeValue = city;
      }
     }
    }
   }

   try {
    if (attributeValue.length() > 0)
     context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));
   } catch (InterruptedException e) {
    // TODO Auto-generated catch block
    e.printStackTrace();
   }

  }

  public void credit_card(String smstxt, String smsId, String phoneNo, String time, Context context) throws IOException {
   attributeName = "credit_card";
   if (smstxt.contains("credit card") || smstxt.contains("creditcard")) {
    priority = 1;
    attributeValue = "Yes";
   } else if (smstxt.contains("card") && (smstxt.contains("payment") || smstxt.contains("receive") || smstxt.contains("changed") || smstxt.contains("blocked") || smstxt.contains("resolved"))) {
    priority = 2;
    attributeValue = "Yes";
   } else if (smstxt.contains("card") && (smstxt.contains("transaction") || smstxt.contains("txns"))) {
    priority = 2;
    attributeValue = "Yes";
   }

   try {
    if (attributeValue.length() > 0) {
     context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));
    }
   } catch (InterruptedException e) {
    // TODO Auto-generated catch block
    e.printStackTrace();
   }

  }

  public void car_loan(String smstxt, String smsId, String phoneNo, String time, String fromNumber, Context context) throws IOException {

   attributeName = "car_loan";
   attributeValue = "Yes";
   String t=time;
   if ((smstxt.contains("car loan") || (smstxt.contains("car") && smstxt.contains("loan"))) && !(smstxt.contains("thank you") && smstxt.contains("clarifications")))
    priority = 1;
    if ((fromNumber.equals("Maruti") || fromNumber.equals("RBLBNK") || fromNumber.equals("GMCBNK") || fromNumber.equals("UnionB") || fromNumber.equals("RATNAK") || fromNumber.equals("CorpBk") || (smstxt.contains("loan") && smstxt.contains("amount") && smstxt.contains("credited")) || (smstxt.contains("loan") && smstxt.contains("mature")) || (smstxt.contains("loan") && smstxt.contains("rate")) || (smstxt.contains("loan") && smstxt.contains("access"))) && !(smstxt.contains("home") || smstxt.contains("gold") || smstxt.contains("tractor")))
    priority = 2;
    if ((smstxt.contains("emi") && !(smstxt.contains("emi card") || smstxt.contains("credit card") || smstxt.contains("cib") || smstxt.contains("sip") || smstxt.contains("gold") || smstxt.contains("mutual fund") || smstxt.contains("folio") || smstxt.contains("bfl") || smstxt.contains("bajaj finserv") || smstxt.contains("pnbhfl") || smstxt.contains("punjab housing") || smstxt.contains("healthcover") || smstxt.contains("exclusive offers") || smstxt.contains("tractor") || smstxt.contains("bike") || smstxt.contains("club mahindra") || smstxt.contains("mercedes") || smstxt.contains("holiday") || smstxt.contains("membership") || smstxt.contains("emi preferred card") || smstxt.contains("muthoot") || smstxt.contains("mortgage")))) priority = 3;

    try {
     if (attributeValue.length() > 0) {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + "car_insurance" + "\t" + attributeValue + "\t" + priority));
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + "saving_account" + "\t" + attributeValue + "\t" + priority));
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + "net_banking" + "\t" + attributeValue + "\t" + priority));
     }
    } catch (InterruptedException e) {
     e.printStackTrace();
    }
   }

   //todo need to review and add more rules
   public void savings_account(String smstxt, String smsId, String phoneNo, String time, Context context) throws IOException {

    attributeName = "saving_account";
    attributeValue = "";
    if (smstxt.contains("savings a/c") || smstxt.contains("savings acct") || smstxt.contains("savings no") || smstxt.contains("saving account") || smstxt.contains("savings acct") || smstxt.contains("savings transactions") || smstxt.contains("savings available balance") || (smstxt.contains("salary") && smstxt.contains("credited")) || smstxt.contains("mobile banking") || smstxt.contains("dear staff, your salary has been deposited")) {
     priority = 1;
     attributeValue = "Yes";
    } else if ((smstxt.contains("salary") && smstxt.contains("credited")) || smstxt.contains("credited to your a/c") || smstxt.contains("salary account") || (smstxt.contains("is credited inr") && (!smstxt.contains("credit card"))) || (smstxt.contains("is debited inr") && (!smstxt.contains("credit card")))) {
     priority = 1;
     attributeValue = "Yes";
    } else if (smstxt.contains("txns") && (!smstxt.contains("credit card") || !smstxt.contains("creditcard"))) {
     priority = 1;
     attributeValue = "Yes";
    } else if ((smstxt.contains("banking") || smstxt.contains("bank")) && (!smstxt.contains("folio") || !smstxt.contains("sip") || !smstxt.contains("mutual fund") || !smstxt.contains("foodie") || !smstxt.contains("sip") || !smstxt.contains("tab banking") || !smstxt.contains("ims summary") || !smstxt.contains("retailid creation") || !smstxt.contains("matrimony"))) {
     priority = 1;
     attributeValue = "Yes";
    } else if ((smstxt.contains("bank") && smstxt.contains("debit")) && (!smstxt.contains("folio") || !smstxt.contains("sip") || !smstxt.contains("mutual fund") || !smstxt.contains("foodie") || !smstxt.contains("tab banking") || !smstxt.contains("ims summary") || !smstxt.contains("retailid creation") || !smstxt.contains("matrimony"))) {
     priority = 1;
     attributeValue = "Yes";
    } else if ((smstxt.contains("bank") && smstxt.contains("otp")) && (!smstxt.contains("cib") || !smstxt.contains("credit card"))) {
     priority = 1;
     attributeValue = "Yes";
    } else if ((smstxt.contains("bank") && (smstxt.contains("credited") || !smstxt.contains("debited")) && (!smstxt.contains("cib") || !smstxt.contains("credit card") || !smstxt.contains("folio number")))) {
     priority = 1;
     attributeValue = "Yes";
    } else if (smstxt.contains("netbanking") && smstxt.contains("internet banking") && (!smstxt.contains("credit card") || !smstxt.contains("creditcard"))) {
     priority = 1;
     attributeValue = "Yes";
    } else if (smstxt.contains("remittance account rejected") || smstxt.contains("last 4 txns")) {
     priority = 3;
     attributeValue = "Yes";
    }

    try {
     if (attributeValue.length() > 0) {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     }
    } catch (InterruptedException e) {
     e.printStackTrace();
    }
   }

   //todo need to review and add more rules
   public void has_kids(String smstxt, String smsId, String phoneNo, String time, String fromNumber, Context context) throws IOException {
    attributeName = "has_kids";
    attributeValue = "";
    String t=time;

    if (smstxt.contains("parent") || smstxt.contains("your child") || (smstxt.contains("school") && !smstxt.contains("staff")) || smstxt.contains("quarter fee") || smstxt.contains("fee of your ward") || smstxt.contains("your ward")) {
     priority = 1;
     attributeValue = "Yes";
    } else if (fromNumber.equals("ldcssms") || fromNumber.equals("hanuman") || fromNumber.equals("magnasoft") || fromNumber.equals("netlink") || fromNumber.equals("vj_vpsps") || fromNumber.equals("stardotstarnon") || fromNumber.equals("rimsmum")) {
     priority = 1;
     attributeValue = "Yes";
    }

    try {
     if (attributeValue.length() > 0) {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));
      //context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + "age" + "\t" + "35-40" + "\t" + priority));
     }
    } catch (InterruptedException e) {
     e.printStackTrace();
    }
   }


   public void income(String smstxt, String smsId, String phoneNo, String t, String fromNumber, Context context) throws IOException {
    String s;
    int salaryamt;
    String attributeName = "income";
    attributeValue = "";
    if (fromNumber.equals("ICICIB")) {
     if (smstxt.contains("salary") && smstxt.contains("is credited with inr")) {
      priority = 1;
      s = smstxt.substring(smstxt.indexOf("is credited with inr") + 20, smstxt.indexOf(" on"));
      salaryamt = Integer.parseInt(s);
      attributeValue = salaryRange(salaryamt);
     }
    } else if (fromNumber.equals("DENABK")) {
     if (smstxt.contains("salary") && smstxt.contains("is credited with rs")) {
      priority = 1;
      s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
      salaryamt = Integer.parseInt(s);
      attributeValue = salaryRange(salaryamt);
     }
    } else if (fromNumber.equals("CorpBk")) {
     if (smstxt.contains("credited with salary inr ")) {
      priority = 1;
      s = smstxt.substring(smstxt.indexOf("credited with salary inr ") + 25, smstxt.indexOf(" on"));
      salaryamt = Integer.parseInt(s);
      attributeValue = salaryRange(salaryamt);
     }
    } else if (fromNumber.equals("LVBANK")) {
     if (smstxt.contains("credited with Rs.") && smstxt.contains("net salary")) {
      priority = 1;
      s = smstxt.substring(smstxt.indexOf("credited with Rs.") + 17, smstxt.indexOf(" on"));
      salaryamt = Integer.parseInt(s);
      attributeValue = salaryRange(salaryamt);
     }
    }
    if (fromNumber.equals("BMCBNK")) {
     if (smstxt.contains(" has been credited by inr ") && smstxt.contains("info- by salary")) {
      priority = 1;
      s = smstxt.substring(smstxt.indexOf("has been credited by inr ") + 25, smstxt.indexOf(" on"));

      salaryamt = Integer.parseInt(s);
      attributeValue = salaryRange(salaryamt);

     }
    } else if (fromNumber.equals("PMCBnk")) {
     if (smstxt.contains("ecs of inr ") && smstxt.contains("trust-salary")) {
      priority = 1;
      s = smstxt.substring(smstxt.indexOf("ecs of inr ") + 11, smstxt.indexOf(" from"));
      salaryamt = Integer.parseInt(s);
      attributeValue = salaryRange(salaryamt);

     }
    }

    if (fromNumber.equals("TJSBNK")) {
     if (smstxt.contains("credited") && smstxt.contains("salaryorigbrcd")) {
      priority = 1;
      //s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
      s = smstxt.substring(11, smstxt.indexOf(" (ref no-  )"));

      salaryamt = Integer.parseInt(s);
      attributeValue = salaryRange(salaryamt);


     }
    } else if (fromNumber.equals("NKGSB")) {
     if (smstxt.contains("salary") && smstxt.contains("credited")) {
      priority = 1;
      //s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
      s = smstxt.substring(37, smstxt.indexOf(" on"));
      salaryamt = Integer.parseInt(s);
      attributeValue = salaryRange(salaryamt);


     }
    }

    if (fromNumber.equals("RSBANK")) {
     if (smstxt.contains("salary") && smstxt.contains("credited")) {
      priority = 1;
      //s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
      s = smstxt.substring(38, smstxt.indexOf(" on"));
      salaryamt = Integer.parseInt(s);
      attributeValue = salaryRange(salaryamt);


     }
    } else if (fromNumber.equals("ACEBNK")) {
     if (smstxt.contains("salary") && smstxt.contains("credited")) {
      priority = 1;
      //s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
      s = smstxt.substring(38, smstxt.indexOf(" on"));
      salaryamt = Integer.parseInt(s);
      attributeValue = salaryRange(salaryamt);

     }
    } else if (fromNumber.equals("GPPJSB")) {
     if (smstxt.contains("salary") && smstxt.contains("credited")) {
      priority = 1;
      //s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
      s = smstxt.substring(39, smstxt.indexOf(" on"));
      salaryamt = Integer.parseInt(s);
      attributeValue = salaryRange(salaryamt);


     }
    }

    if (fromNumber.equals("NNSBANK")) {
     if (smstxt.contains("salary") && smstxt.contains("credited")) {
      priority = 1;
      //s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
      s = smstxt.substring(65, smstxt.indexOf(" on"));
      salaryamt = Integer.parseInt(s);
      attributeValue = salaryRange(salaryamt);



     }
    } else if (fromNumber.equals("SUDICO")) {
     if (smstxt.contains("salary") && smstxt.contains("credited")) {
      priority = 1;
      //s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
      s = smstxt.substring(37, smstxt.indexOf(". your"));
      salaryamt = Integer.parseInt(s);
      attributeValue = salaryRange(salaryamt);


     }
    } else if (fromNumber.equals("MCCDAY")) {
     if (smstxt.contains("salary") && smstxt.contains("credited")) {
      priority = 1;
      //s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
      s = smstxt.substring(22, smstxt.indexOf(" for"));
      salaryamt = Integer.parseInt(s);
      attributeValue = salaryRange(salaryamt);


     }
    } else if (fromNumber.equals("BCCBNK")) {
     if (smstxt.contains("salary") && smstxt.contains("credited")) {
      priority = 1;
      //s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
      s = smstxt.substring(46, smstxt.indexOf(" on"));
      salaryamt = Integer.parseInt(s);
      attributeValue = salaryRange(salaryamt);


     }
    } else if (fromNumber.equals("SSBMUM")) {
     if (smstxt.contains("salary") && smstxt.contains("credited")) {
      priority = 1;
      //s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
      s = smstxt.substring(39, smstxt.indexOf(" On"));
      salaryamt = Integer.parseInt(s);
      attributeValue = salaryRange(salaryamt);


     }
    } else if (fromNumber.equals("BLBNK")) {
     if (smstxt.contains("salary") && smstxt.contains("credited")) {
      priority = 1;
      //s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
      s = smstxt.substring(54, smstxt.indexOf(" on"));
      salaryamt = Integer.parseInt(s);
      attributeValue = salaryRange(salaryamt);


     }
    } else if (fromNumber.equals("PNB")) {
     if (smstxt.contains("salary") && smstxt.contains("credited")) {
      priority = 1;
      //s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
      s = smstxt.substring(37, smstxt.indexOf(","));
      salaryamt = Integer.parseInt(s);
      attributeValue = salaryRange(salaryamt);


     }
    } else if (fromNumber.equals("VBANK")) {
     if (smstxt.contains("salary") && smstxt.contains("credited")) {
      priority = 1;
      //s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
      s = smstxt.substring(29, smstxt.indexOf(" on"));
      salaryamt = Integer.parseInt(s);
      attributeValue = salaryRange(salaryamt);


     }
    } else if (fromNumber.equals("UBANK")) {
     if (smstxt.contains("salary") && smstxt.contains("credited")) {
      priority = 1;
      //s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
      s = smstxt.substring(40, smstxt.indexOf(" towards"));
      salaryamt = Integer.parseInt(s);
      attributeValue = salaryRange(salaryamt);



     }
    } else if (fromNumber.equals("TBSREW")) {
     if (smstxt.contains("salary") && smstxt.contains("credited")) {
      priority = 1;
      //s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
      s = smstxt.substring(35, smstxt.indexOf(" has"));
      System.out.println(s);
      salaryamt = Integer.parseInt(s);
      attributeValue = salaryRange(salaryamt);


     }
    } else if (fromNumber.equals("AxisBk")) {
     if (smstxt.contains("salary") || smstxt.contains("credited")) {
      priority = 1;
      //s = smstxt.substring(smstxt.indexOf("is credited with Rs.") + 20, smstxt.indexOf(" towards"));
      s = smstxt.substring(smstxt.indexOf("is credited rs ") + 15, smstxt.indexOf(" on"));
      salaryamt = Integer.parseInt(s);
      attributeValue = salaryRange(salaryamt);
     }
    }


    try {
     if (attributeValue.length() > 0) {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + "health_insurance" + "\t" + "yes" + "\t" + priority));
     }
    } catch (InterruptedException e) {
     e.printStackTrace();
    }
   }

   public String salaryRange(int salary) {
    int ctc = salary * 12;
    if (ctc >= 0 && ctc <= 300000) {
     return "0-3L";
    } else if (ctc > 300000 && ctc <= 600000) {
     return "3-6L";
    } else if (ctc > 600000 && ctc <= 1000000) {
     return "6-10L";
    } else if (ctc > 1000000 && ctc <= 1500000) {
     return "10-15L";
    } else if (ctc > 1500000 && ctc <= 2000000)
     return "15-20L";
    else if (ctc > 2000000 && ctc <= 2500000)
     return "20-25L";
    else if (ctc > 2500000 && ctc <= 4000000)
     return "25-40L";
    else if (ctc > 4000000 && ctc <= 6000000)
     return "40-60L";
    else if (ctc > 6000000 && ctc <= 10000000)
     return "60L-1CR";
    else
     return "1CR+";
   }

   //todo need to review and add more rules
   public void life_insurance(String smstxt, String smsId, String phoneNo, String time, String fromNumber, Context context) throws IOException {
    attributeName = "life_insurance";
    attributeValue = "";
    if (smstxt.contains("life insurance") || smstxt.contains("life policy") || smstxt.contains("pru policy") || smstxt.contains("indiafirst")) {
     priority = 1;
     attributeValue = "Yes";
    }
    if (fromNumber.equals("SUDLIF")) {
     priority = 2;
     attributeValue = "Yes";
    }
    try {
     if (attributeValue.length() > 0)
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));
    } catch (InterruptedException e) {
     e.printStackTrace();
    }
   }

   //todo need to review and add more rules
   public void car_insurance(String smstxt, String smsId, String phoneNo, String time, String fromNumber, Context context) throws IOException {

    attributeName = "car_insurance";
    attributeValue = "";
    if (smstxt.contains("maruti insurance")) {
     priority = 1;
     attributeValue = "Yes";
    }
    try {
     if (attributeValue.length() > 0)
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));
    } catch (InterruptedException e) {
     e.printStackTrace();
    }
   }

   //todo need to review and add more rules
   public void apparel(String smstxt, String smsId, String phoneNo, String time, Context context) throws IOException {

    attributeName = "apparel";
    attributeValue = "";
    if (smstxt.contains(" tops ") || smstxt.contains("tees") || smstxt.contains("shirt") || smstxt.contains("dress") || smstxt.contains("sweater") || smstxt.contains("jacket") || smstxt.contains("sweatshirt") || smstxt.contains("trouser") || smstxt.contains("jeans") || smstxt.contains("skirt") || smstxt.contains("legging") || smstxt.contains("jegging") || smstxt.contains("tunic") || smstxt.contains("kurtas") || smstxt.contains("kurtis") || smstxt.contains("suit") || smstxt.contains("sarees") || smstxt.contains("salwar") || smstxt.contains("churidar") || smstxt.contains("bags") || smstxt.contains("clutches") || smstxt.contains("totes") || smstxt.contains("wallet") || smstxt.contains("shoes") || smstxt.contains("sandals") || smstxt.contains("bellies") || smstxt.contains("boot") || smstxt.contains("sneaker") || smstxt.contains("loafer") || smstxt.contains("watches") || smstxt.contains("sunglasses") || smstxt.contains("belts") || smstxt.contains("blazer") || smstxt.contains("bags")) {
     priority = 1;
     attributeValue = "Yes";

    }
    try {
     if (attributeValue.length() > 0)
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));
    } catch (InterruptedException e) {
     e.printStackTrace();
    }
   }


   public void gadgets(String smstxt, String smsId, String phoneNo, String time, Context context) throws IOException {

    attributeName = "gadgets";
    attributeValue = "";
    if (smstxt.contains("speaker") || smstxt.contains("bluetooth") || smstxt.contains("soundbar") || smstxt.contains("ipod") || smstxt.contains("mp3 player") || smstxt.contains("mp4 player") || smstxt.contains("video player") || smstxt.contains("media streaming") || smstxt.contains("device") || smstxt.contains("fm radio") || smstxt.contains("boom box") || smstxt.contains("video glasses") || smstxt.contains("remote controller") || smstxt.contains("voltage stabilizers") || smstxt.contains("camera") || smstxt.contains("canon") || smstxt.contains("nikon") || smstxt.contains("sony") || smstxt.contains("go pro") || smstxt.contains("cell phone") || smstxt.contains("mobile phone") || smstxt.contains("tripod") || smstxt.contains("memory card") || smstxt.contains("binocular") || smstxt.contains("battery") || smstxt.contains("memory card") || smstxt.contains("gamepad") || smstxt.contains("headset") || smstxt.contains("headphone") || smstxt.contains("gaming mice") || smstxt.contains("intex") || smstxt.contains("lava") || smstxt.contains("samsung") || smstxt.contains("micromax") || smstxt.contains("karbon") || smstxt.contains("adcom") || smstxt.contains("hitech") || smstxt.contains("i kall") || smstxt.contains("zopo") || smstxt.contains("airtyme") || smstxt.contains("akai") || smstxt.contains("alkatel") || smstxt.contains("apple") || smstxt.contains("arise") || smstxt.contains("asus") || smstxt.contains("blackberry") || smstxt.contains("bq mobile") || smstxt.contains("byond") || smstxt.contains("celkon") || smstxt.contains("datawind") || smstxt.contains("emerin") || smstxt.contains("formin") || smstxt.contains("gionee") || smstxt.contains("go hello") || smstxt.contains("hsl") || smstxt.contains("htc") || smstxt.contains("huawei") || smstxt.contains("iball") || smstxt.contains("ice x") || smstxt.contains("ismart") || smstxt.contains("geotex") || smstxt.contains("kenxinda") || smstxt.contains("kestrel") || smstxt.contains("lenovo") || smstxt.contains("LG") || smstxt.contains("maxx") || smstxt.contains("microsoft") || smstxt.contains("mitashi") || smstxt.contains("motorola") || smstxt.contains("nexg") || smstxt.contains("nokia") || smstxt.contains("onida") || smstxt.contains("oppo") || smstxt.contains("panasonic") || smstxt.contains("philip") || smstxt.contains("reliance") || smstxt.contains("salora") || smstxt.contains("Saral Sigmatel") || smstxt.contains("smartplay") || smstxt.contains("sony") || smstxt.contains("spice") || smstxt.contains("subway") || smstxt.contains("swipe") || smstxt.contains("tmax") || smstxt.contains("trio mobile") || smstxt.contains("vbera") || smstxt.contains("videocon") || smstxt.contains("vinner") || smstxt.contains("vox") || smstxt.contains("wham") || smstxt.contains("wynncom") || smstxt.contains("xccess") || smstxt.contains("xelectron") || smstxt.contains("xillion") || smstxt.contains("zen") || smstxt.contains("zte") || smstxt.contains("zync") || smstxt.contains("tablet") || smstxt.contains("power bank") || smstxt.contains("batteries") || smstxt.contains("charger") || smstxt.contains("data cable") || smstxt.contains("memory card") || smstxt.contains("video game") || smstxt.contains("desktop") || smstxt.contains("mouse") || smstxt.contains("keyboard") || smstxt.contains("monitor") || smstxt.contains("webcam") || smstxt.contains("projector") || smstxt.contains("printer") || smstxt.contains("cables") || smstxt.contains("router") || smstxt.contains("smart watch") || smstxt.contains("data card") || smstxt.contains("television") || smstxt.contains("trimmer") || smstxt.contains("philips") || smstxt.contains("hair dryers")) {
     priority = 1;
     attributeValue = "Yes";
    }
    try {
     if (attributeValue.length() > 0)
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));
    } catch (InterruptedException e) {
     e.printStackTrace();
    }
   }

   public void sports(String smstxt, String smsId, String phoneNo, String time, Context context) throws IOException {
    attributeName = "sports";
    attributeValue = "";
    if (smstxt.contains("adidas") || smstxt.contains("sparx") || smstxt.contains("puma") || smstxt.contains("hrx") || smstxt.contains("nike") || smstxt.contains("reebok") || smstxt.contains("yonex") || smstxt.contains("nivia") || smstxt.contains("headly") || smstxt.contains("cricket") || smstxt.contains("football") || smstxt.contains("badminton") || smstxt.contains("tennis") || smstxt.contains("swimming") || smstxt.contains("basketball") || smstxt.contains("boxing") || smstxt.contains("cycling") || smstxt.contains("camping") || smstxt.contains("hiking") || smstxt.contains("skating") || smstxt.contains("hockey") || smstxt.contains("volley ball") || smstxt.contains("squash") || smstxt.contains("golf") || smstxt.contains("billiad") || smstxt.contains("pool")) {
     priority = 1;
     attributeValue = "Yes";
    }

   }

   public void movies(String smstxt, String smsId, String phoneNo, String time, String fromNumber, Context context) throws IOException {

    attributeName = "movies";
    attributeValue = "";
    if (fromNumber == "BMSHOW") {
     priority = 1;
     attributeValue = "Yes";
    }
    try {
     if (attributeValue.length() > 0)
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));
    } catch (InterruptedException e) {
     e.printStackTrace();
    }
   }

   //todo need to review and add more rules
   public void frequent_traveller(String smstxt, String smsId, String phoneNo, String time, String fromNumber, Context context) throws IOException {

    attributeName = "freq_traveller";
    attributeValue = "maybe";
    if (fromNumber == "ONEWAY" && smstxt.contains("from") && smstxt.contains("airport") && smstxt.contains("confirmed")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }
    if (fromNumber == "GOLTRP" && smstxt.contains("flight") && smstxt.contains("confirmed") && smstxt.contains("departing")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }
    if (fromNumber == "KESARI" && smstxt.contains("confirmed") && smstxt.contains("returning") && smstxt.contains("flight")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }
    if (fromNumber == "KKTRVL" && smstxt.contains("booking") && smstxt.contains("airport") && smstxt.contains("confirmed") && smstxt.contains("flight")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }
    }

    if (fromNumber == "FullOn" && smstxt.contains("passenger") && smstxt.contains("terminal") && smstxt.contains("confirmed")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }
    }

    if (fromNumber == "BUSTVL" && smstxt.contains("pnr") && smstxt.contains("departing") && smstxt.contains("flight")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }
    }
   }

   //todo need to review and add more rules
   public void frequent_shopper(String st, String smsId, String phoneNo, String time, String fromNumber, Context context) throws IOException {
    String smstxt = st;
   // String smsId = smsId;
  //  String phoneNo = phoneNo;
    String t = time;
    attributeName = "freq_shopper";
    attributeValue = "maybe";
    if (fromNumber == "Dotzot" && smstxt.contains("ordered") && smstxt.contains("delivered")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }

    if (fromNumber == "FARMSB" && smstxt.contains("ordered") && smstxt.contains("delivered")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }

    if (fromNumber == "REEBOK" && smstxt.contains("ordered") && smstxt.contains("delivered")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }

    if (fromNumber == "ADIDAS" && smstxt.contains("ordered") && smstxt.contains("delivered")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }

    if (fromNumber == "HBUDDY" && smstxt.contains("ordered") && smstxt.contains("delivered")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }
    if (fromNumber == "FabFur" && smstxt.contains("ordered") && smstxt.contains("delivered")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }
    if (fromNumber == "56070" && smstxt.contains("ordered") && smstxt.contains("delivered")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }
    if (fromNumber == "HPSTCH" && smstxt.contains("ordered") && smstxt.contains("delivered")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }
    if (fromNumber == "MSSDCL" && smstxt.contains("ordered") && smstxt.contains("delivered")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }
    if (fromNumber == "PLANTM" && smstxt.contains("ordered") && smstxt.contains("delivered")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }
    if (fromNumber == "HONEST" && smstxt.contains("ordered") && smstxt.contains("delivered")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }
    if (fromNumber == "BUCKET" && smstxt.contains("ordered") && smstxt.contains("delivered")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }
    if (fromNumber == "zobelo" && smstxt.contains("ordered") && smstxt.contains("delivered")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }
    if (fromNumber == "eshops" && smstxt.contains("ordered") && smstxt.contains("delivered")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }
    if (fromNumber == "GIFTEZ" && smstxt.contains("ordered") && smstxt.contains("delivered")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }
    if (fromNumber == "JUPTSH" && smstxt.contains("ordered") && smstxt.contains("delivered")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }
    if (fromNumber == "FABONE" && smstxt.contains("ordered") && smstxt.contains("delivered")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }
    if (fromNumber == "MFTREE" && smstxt.contains("ordered") && smstxt.contains("delivered")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }
    if (fromNumber == "GIFTEZ" && smstxt.contains("ordered") && smstxt.contains("delivered")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }
    if (fromNumber == "TRNDIN" && smstxt.contains("ordered") && smstxt.contains("delivered")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }
    if (fromNumber == "ZOPBZR" && smstxt.contains("delivered")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }
    if (fromNumber == "RedExp" && smstxt.contains("ordered") && smstxt.contains("out of delivery")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }

    if (fromNumber == "Myntra" && smstxt.contains("shipment") && smstxt.contains("delivery")) {
     priority = 1;
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + t + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }
   }

   public void health_insurance(String st, String smsId, String phoneNo, String time, String fromNumber, Context context) throws IOException {
    attributeName = "health_insurance";
    attributeValue = "";
    String smstxt=st;

    if (fromNumber.equals("SUDLIF")) {
     priority = 2;
     attributeValue = "Yes";
    }

    if (smstxt.contains("health insurance") || smstxt.contains("medical insurance") || smstxt.contains("health policy") || smstxt.contains("lombard policy")) {
     priority = 1;
     attributeValue = "Yes";
    }
    try {
     if (attributeValue.length() > 0)
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));
    } catch (InterruptedException e) {
     e.printStackTrace();
    }
   }

   public void mutual_fund(String smstxt, String smsId, String phoneNo, String time, Context context) throws IOException {

    attributeName = "mutual_fund";
    attributeValue = "";

    if (smstxt.contains("mutual fund") || (smstxt.contains("folio") && !smstxt.contains("insurance")) || smstxt.contains("mutualfund") || smstxt.contains("sip")) {
     priority = 1;
     attributeValue = "Yes";
    }
    try {
     if (attributeValue.length() > 0) {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + "saving_account" + "\t" + "yes" + "\t" + priority));
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + "net_banking" + "\t" + "yes" + "\t" + priority));

     }
    } catch (InterruptedException e) {
     e.printStackTrace();
    }

   }

   public void home_loan(String smstxt, String smsId, String phoneNo, String time, String fromNumber, Context context) throws IOException {

    attributeName = "home_loan";
    attributeValue = "";
      if (fromNumber.equals("CorpBk") || fromNumber.equals("RBLBNK") || fromNumber.equals("GMCBNK") || fromNumber.equals("UnionB") || fromNumber.equals("RATNAK")) {
     priority = 2;
     attributeValue = "Yes";
    }  if (smstxt.contains("home loan") || (smstxt.contains("loan account") || smstxt.contains("loan acct") || smstxt.contains("emi for acct")) || smstxt.contains("emi for your loan") || (smstxt.contains("vide receipt") || smstxt.contains("icici"))) //home loan
    {
     priority = 1;
     attributeValue = "Yes";
    }  if (smstxt.contains("loan application") && ((smstxt.contains("approved") && !smstxt.contains("auto approved")) || smstxt.contains("disbursement"))) //home loan
    {
     priority = 1;
     attributeValue = "Yes";
    }  if (smstxt.contains("emi") && (!smstxt.contains("emi card") || !smstxt.contains("credit card") || !smstxt.contains("cib") || !smstxt.contains("sip") || !smstxt.contains("gold") || !smstxt.contains("mutual fund") || !smstxt.contains("folio") || !smstxt.contains("bfl") || !smstxt.contains("bajaj finserv") || !smstxt.contains("bajaj auto") || !smstxt.contains("nano credit") || !smstxt.contains("tmf") || !smstxt.contains("bharatbenz") || !smstxt.contains("capital first") || !smstxt.contains("tata motors") || !smstxt.contains("health cover") || !smstxt.contains("exclusive offers") || !smstxt.contains("tractor") || !smstxt.contains("auto") || !smstxt.contains("bike") || !smstxt.contains("club mahindra") || !smstxt.contains("mercedes") || !smstxt.contains("holiday") || !smstxt.contains("membership") || !smstxt.contains("emi preferred card"))) //mutual fund
    {
     priority = 2;
     attributeValue = "Yes";
    }

    try {
     if (attributeValue.length() > 0) {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + "savings_account" + "\t" + "yes" + "\t" + priority));
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + "net_banking" + "\t" + "yes" + "\t" + priority));

     }
    } catch (InterruptedException e) {
     e.printStackTrace();
    }
   }

   public void internet_banking(String smstxt, String smsId, String phoneNo, String time, Context context) throws IOException {
    attributeName = "net_banking";
    attributeValue = "";
    if (smstxt.contains("internet banking") || smstxt.contains("inter net bkg.") || smstxt.contains("netbanking") || smstxt.contains("e_banking") || smstxt.contains("net banking") || smstxt.contains("demat account")) {
     priority = 1;
     attributeValue = "Yes";
    }

    try {
     if (attributeValue.length() > 0)
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));
    } catch (InterruptedException e) {
     e.printStackTrace();
    }
   }

   public void age(String smstxt, String smsId, String phoneNo, String time, Context context) throws IOException {

    attributeName = "age";

    if ((smstxt.contains("prepare") && smstxt.contains(" iit ")) || smstxt.contains("jee books") || smstxt.contains("ssc board exam") || smstxt.contains("board examination") || smstxt.contains("10th class") || smstxt.contains("12th class")) {
     priority = 1;
     attributeValue = "under 18";
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + "health_insurance" + "\t" + "no" + "\t" + priority));
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + "has_kids" + "\t" + "no" + "\t" + priority));
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + "car_insurance" + "\t" + "no" + "\t" + priority));
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + "mutual_fund" + "\t" + "no" + "\t" + priority));
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + "credit_card" + "\t" + "no" + "\t" + priority));
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + "car_loan" + "\t" + "no" + "\t" + priority));
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + "home_loan" + "\t" + "no" + "\t" + priority));
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + "income" + "\t" + "0" + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    } else if ((smstxt.contains("prepare") && smstxt.contains(" mba ")) || smstxt.contains("dear mba/pgdm aspirant") || smstxt.contains("freshers") || smstxt.contains("cat prep") || smstxt.contains("crack cat") || smstxt.contains("cat exam") || (smstxt.contains("admission open") && (smstxt.contains("diploma") || smstxt.contains("college")))) {
     priority = 1;
     attributeValue = "18-25";
     try {
      if (attributeValue.length() > 0) {
       context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));
       context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + "health_insurance" + "\t" + "no" + "\t" + priority));
       context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + "has_kids" + "\t" + "no" + "\t" + priority));
       context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + "car_insurance" + "\t" + "no" + "\t" + priority));
       context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + "mutual_fund" + "\t" + "no" + "\t" + priority));
       context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + "car_loan" + "\t" + "no" + "\t" + priority));
       context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + "home_loan" + "\t" + "no" + "\t" + priority));
      }
     } catch (InterruptedException e) {
      e.printStackTrace();
     }
    } else if (smstxt.contains("matrimony")) { //todo need to add more rules
     priority = 2;
     attributeValue = "25-30";
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + "has_kids" + "\t" + "no" + "\t" + priority));


     } catch (InterruptedException e) {
      e.printStackTrace();
     }

    } else if (smstxt.contains("child education plan")) {
     priority = 2;
     attributeValue = "35-40";
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + "health_insurance" + "\t" + "yes" + "\t" + priority));
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + "has_kids" + "\t" + "yes" + "\t" + priority));


     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    } else if (smstxt.contains("admission open") && smstxt.contains("phd")) {
     priority = 2;
     attributeValue = "25-30";
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    } else if (smstxt.contains("senior citizens services")) {
     priority = 1;
     attributeValue = "50+";
     try {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + "health_insurance" + "\t" + "yes" + "\t" + priority));
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + "has_kids" + "\t" + "yes" + "\t" + priority));

     } catch (InterruptedException e) {

      e.printStackTrace();
     }

    }
   }

   public void gender(String smstxt, String smsId, String phoneNo, String time, Context context) throws IOException {

    attributeName = "gender";
    attributeValue = "";
    if (smstxt.contains(" mr.") || smstxt.contains("dear sir") || smstxt.contains("dear salesperson") || smstxt.contains("hi sir") || smstxt.contains("hi uncle") || smstxt.contains("hi bhaiya") || smstxt.contains("hi bro") || smstxt.contains("hi brother") || smstxt.contains("hi father") || smstxt.contains("hi papa") || smstxt.contains("hi dad") || smstxt.contains("hi pa") || smstxt.contains("hi dady") || smstxt.contains("hi jiju") || smstxt.contains("hi mama") || smstxt.contains("hi chacha") || smstxt.contains("hi tau") || smstxt.contains("hi bhai") || smstxt.contains("hi ladke") || smstxt.contains("hi mr.") || smstxt.contains("hey sir") || smstxt.contains("hey uncle") || smstxt.contains("hey bhaiya") || smstxt.contains("hey bro") || smstxt.contains("hey didi") || smstxt.contains("hey brother") || smstxt.contains("hey father") || smstxt.contains("hey papa") || smstxt.contains("hey dad") || smstxt.contains("hey pa") || smstxt.contains("hi dady") || smstxt.contains("hey jiju") || smstxt.contains("hey mama") || smstxt.contains("hey chacha") || smstxt.contains("hey tau") || smstxt.contains("hey bhai") || smstxt.contains("hey ladke") || smstxt.contains("hello sir") || smstxt.contains("hello uncle") || smstxt.contains("hello bhaiya") || smstxt.contains("hello bro") || smstxt.contains("hello brother") || smstxt.contains("hello father") || smstxt.contains("hello papa") || smstxt.contains("hello dad") || smstxt.contains("hello pa ") || smstxt.contains("hello dady") || smstxt.contains("hello jiju") || smstxt.contains("hello mama") || smstxt.contains("hello chacha") || smstxt.contains("hello tau") || smstxt.contains("hello bhai") || smstxt.contains("hello ladke") || smstxt.contains("aur mote") || smstxt.contains("aur londe") || (smstxt.contains("kumar") && !smstxt.contains("kumari")) || smstxt.contains("mohd") || smstxt.contains("mohamad") || smstxt.contains("muhammad")) // gender attributeName
    {
     priority = 1;
     attributeValue = "male";
    } else if (smstxt.contains("hi mumma") || smstxt.contains("hi mummy") || smstxt.contains("hi mom") || smstxt.contains("hi di") || smstxt.contains("hi sis") || smstxt.contains("hi didi") || smstxt.contains("hi bua") || smstxt.contains("hi bhabhi") || smstxt.contains("hi behen ") || smstxt.contains("hi behna") || smstxt.contains("hi behenji") || smstxt.contains("hi aunty") || smstxt.contains("hi chachi") || smstxt.contains("hi ladki") || smstxt.contains("hi mam") || smstxt.contains("hi madam") || smstxt.contains("hey mumma") || smstxt.contains("hey mummy") || smstxt.contains("hey mom") || smstxt.contains("hey di") || smstxt.contains("hey sis") || smstxt.contains("hey didi") || smstxt.contains("hey bua") || smstxt.contains("hey bhabhi") || smstxt.contains("hey behen ") || smstxt.contains("hey behna") || smstxt.contains("hey behenji") || smstxt.contains("hey aunty") || smstxt.contains("hey chachi") || smstxt.contains("hey ladki") || smstxt.contains("hey mam") || smstxt.contains("hey madam") || smstxt.contains("hey madam") || smstxt.contains("dear mrs.") || smstxt.contains("dear mam") || smstxt.contains("dear madam") || smstxt.contains("dear ms.") || smstxt.contains("aur moti") || smstxt.contains("kumari") || smstxt.contains("kaur")) {
     priority = 1;
     attributeValue = "female";
    } else if ((smstxt.startsWith("dear")) && (!smstxt.contains("dear customer") || !smstxt.contains("dear investor") || !smstxt.contains("dear counseling") || !smstxt.contains("dear valued") || !smstxt.contains("dear club") || !smstxt.contains("dear guest") || !smstxt.contains("dear incumbent") || !smstxt.contains("dear aviva") || !smstxt.contains("dear sir/mam") || !smstxt.contains("dear axis") || !smstxt.contains("dear partner") || !smstxt.contains("dear iterm") || !smstxt.contains("dear cbm/bm") || !smstxt.contains("dear msdian") || !smstxt.contains("dear csp") || !smstxt.contains("dear policyholder") || !smstxt.contains("dear channel") || !smstxt.contains("dear d2h") || !smstxt.contains("dear lvb") || !smstxt.contains("dear administrator") || !smstxt.contains("dear associate") || !smstxt.contains("dear parent") || !smstxt.contains("dear sir/madam") || !smstxt.contains("dear student") || !smstxt.contains("dear met") || !smstxt.contains("dear tmf") || !smstxt.contains("dear your") || !smstxt.contains("dear abcd") || !smstxt.contains("dear aegon") || !smstxt.contains("dear aspirant") || !smstxt.contains("dear asm") || !smstxt.contains("dear auditor") || !smstxt.contains("dear auro") || !smstxt.contains("dear bfl") || !smstxt.contains("dear bh") || !smstxt.contains("dear bussiness") || !smstxt.contains("dear candidates") || !smstxt.contains("dear cl84") || !smstxt.contains("dear crew") || !smstxt.contains("dear cust") || !smstxt.contains("dear dealor") || !smstxt.contains("dear deposit") || !smstxt.contains("dear dolphin") || !smstxt.contains("dear donor") || !smstxt.contains("dear dp") || !smstxt.contains("dear dotcabs") || !smstxt.contains("dear driver") || !smstxt.contains("dear donor") || !smstxt.contains("dear emp") || !smstxt.contains("dear fac") || !smstxt.contains("dear fino") || !smstxt.contains("dear forum") || !smstxt.contains("dear friend") || !smstxt.contains("dear gamebuddy") || !smstxt.contains("dear gas") || !smstxt.contains("dear gsc") || !smstxt.contains("dear gold") || !smstxt.contains("dear guardian") || !smstxt.contains("dear health") || !smstxt.contains("dear host") || !smstxt.contains("dear indigo") || !smstxt.contains("dear instructor") || !smstxt.contains("dear jalan") || !smstxt.contains("dear key") || !smstxt.contains("dear kids") || !smstxt.contains("dear la") || !smstxt.contains("dear learner") || !smstxt.contains("dear fac") || !smstxt.contains("dear maben") || !smstxt.contains("dear member") || !smstxt.contains("dear passenger") || !smstxt.contains("dear pensioner") || !smstxt.contains("dear phama") || !smstxt.contains("dear prerana") || !smstxt.contains("dear principal") || !smstxt.contains("dear r.m") || !smstxt.contains("dear relationship") || !smstxt.contains("dear resident") || !smstxt.contains("dear retailer") || !smstxt.contains("dear rhs") || !smstxt.contains("dear rh") || !smstxt.contains("dear rm") || !smstxt.contains("dear rs name") || !smstxt.contains("dear rupantaran") || !smstxt.contains("dear seller") || !smstxt.contains("dear sir / madam") || !smstxt.contains("dear sm/oh") || !smstxt.contains("dear spice") || !smstxt.contains("dear so,") || !smstxt.contains("dear sp,") || !smstxt.contains("dear sri sai") || !smstxt.contains("dear staff") || !smstxt.contains("dear stockholding") || !smstxt.contains("dear subscriber") || !smstxt.contains("dear surveyor") || !smstxt.contains("dear teacher") || !smstxt.contains("dear team") || !smstxt.contains("dear tech") || !smstxt.contains("dear tmf") || !smstxt.contains("dear test") || !smstxt.contains("dear tpddl") || !smstxt.contains("dear tractor") || !smstxt.contains("dear trainer") || !smstxt.contains("dear trustee") || !smstxt.contains("dear ubiuser") || !smstxt.contains("dear ucoites") || !smstxt.contains("dear user") || !smstxt.contains("dear vaish") || !smstxt.contains("dear valuefirst") || !smstxt.contains("dear vb") || !smstxt.contains("dear vendor") || !smstxt.contains("dear visitor") || !smstxt.contains("dear viproite") || !smstxt.contains("dear xxx") || !smstxt.contains("dear xyz") || !smstxt.contains("dear zh") || !smstxt.contains("dear member") || !smstxt.contains("dear privilege") || !smstxt.contains("dear patient") || !smstxt.contains("dear prof") || !smstxt.contains("dear rbm") || !smstxt.contains("dear reader") || !smstxt.contains("dear saving") || !smstxt.contains("dear travel") || !smstxt.contains("dear trustline") || !smstxt.contains("dear vp") || !smstxt.contains("dear consumer") || !smstxt.contains("dear pnm") || !smstxt.contains("dear distributor") || !smstxt.contains("dear colleague") || !smstxt.contains("dear <<xyz>>") || !smstxt.contains("dear <>") || !smstxt.contains("dear __") || !smstxt.contains("dear abc") || !smstxt.contains("dear admin") || !smstxt.contains("dear advisor") || !smstxt.contains("dear adsf") || !smstxt.contains("dear agent") || !smstxt.contains("dear agr74") || !smstxt.contains("dear and7") || !smstxt.contains("dear and8") || !smstxt.contains("dear applicant") || !smstxt.contains("dear arli") || !smstxt.contains("dear bsc") || !smstxt.contains("dear bdm") || !smstxt.contains("dear beneficiary") || !smstxt.contains("dear bm") || !smstxt.contains("dear branch") || !smstxt.contains("dear broker") || !smstxt.contains("dear bsm") || !smstxt.contains("dear buisness") || !smstxt.contains("dear cabin") || !smstxt.contains("dear call") || !smstxt.contains("dear card") || !smstxt.contains("dear cavins") || !smstxt.contains("dear manager"))) {
     priority = 2;
     int len, k, r;
     r = smstxt.indexOf("dear ") + 5;

     len = smstxt.length();
     for (k = r; k < len; k++) {
      if (smstxt.charAt(k) == ' ' || smstxt.charAt(k) == ',') {
       char c = smstxt.charAt(k - 1);
       if (c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u') {
        attributeValue = "female";
       } else {
        attributeValue = "male";
       }

      }
     }
    } else if ((smstxt.startsWith("hi")) && (!smstxt.contains("hi loan") || !smstxt.contains("hi .y") || !smstxt.contains("hi thanks") || !smstxt.contains("hi ! y") || !smstxt.contains("hi hfrp") || !smstxt.contains("hi tssss") || !smstxt.contains("hi this") || !smstxt.contains("hi there") || !smstxt.contains("hi test") || !smstxt.contains("hi the") || !smstxt.contains("hi (name)") || !smstxt.contains("hi -wel") || !smstxt.contains("hi - wel") || !smstxt.contains("hi - your") || !smstxt.contains("hi . Your"))) {
     priority = 2;
     int len, k, r;
     r = smstxt.indexOf("hi ") + 3;

     len = smstxt.length();
     for (k = r; k < len; k++) {
      if (smstxt.charAt(k) == ' ' || smstxt.charAt(k) == ',' || smstxt.charAt(k) == '!') {
       char c = smstxt.charAt(k - 1);
       if (c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u') {
        attributeValue = "female";
       } else {
        attributeValue = "male";
       }

      }
     }

    } else if ((smstxt.startsWith("hello")) && (!smstxt.contains("hello, the") || !smstxt.contains("hello!we") || !smstxt.contains("hello!you") || !smstxt.contains("Hello abcd") || !smstxt.contains("hello!subsribe") || !smstxt.contains("hello,we") || !smstxt.contains("helloyour") || !smstxt.contains("hello! t") || !smstxt.contains("hello t ") || !smstxt.contains("hello member") || !smstxt.contains("hello allotment") || !smstxt.contains("hello atm") || !smstxt.contains("hello, you") || !smstxt.contains("hello magzine") || !smstxt.contains("hello,your") || !smstxt.contains("hello, this") || !smstxt.contains("hello xyz") || !smstxt.contains("hello,a travel") || !smstxt.contains("hello, one") || !smstxt.contains("hello, kindly") || !smstxt.contains("hello. Please") || !smstxt.contains("hello, credit") || !smstxt.contains("hello@") || !smstxt.contains("hello, please") || !smstxt.contains("hello, item") || !smstxt.contains("hello from") || !smstxt.contains("hello, i") || !smstxt.contains("hello all") || !smstxt.contains("Hello! Your") || !smstxt.contains("Hello!we") || !smstxt.contains("Hello all") || !smstxt.contains("Hello,as") || !smstxt.contains("hello. kindly"))) {
     priority = 2;
     int len, k, r;
     r = smstxt.indexOf("hello ") + 6;

     len = smstxt.length();
     for (k = r; k < len; k++) {
      if (smstxt.charAt(k) == ' ' || smstxt.charAt(k) == ',') {
       char c = smstxt.charAt(k - 1);
       if (c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u') {
        attributeValue = "female";
       } else {
        attributeValue = "male";
       }

      }
     }
    }

    try {
     if (attributeValue.length() > 0) {
      context.write(new Text(smsId), new Text(phoneNo + "\t" + time + "\t" + attributeName + "\t" + attributeValue + "\t" + priority));
     }
    } catch (InterruptedException e) {
     // TODO Auto-generated catch block
     e.printStackTrace();
    }
   }
  }