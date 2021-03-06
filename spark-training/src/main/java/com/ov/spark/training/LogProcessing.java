package com.ov.spark.training;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

public class LogProcessing {

	public static void processingLog( String iFilename )
	{
		// Define a configuration to use to interact with Spark
		System.setProperty("hadoop.home.dir", "C:/Hadoop");
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Work Count App");

		// Create a Java version of the Spark Context from the configuration
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load the input data, which is a text file read from the command line
		JavaRDD<String> input = sc.textFile( iFilename );

		// filter only the line contain the IP address
		JavaRDD<String> text = input.filter(l -> filterAddressIp(l));
		JavaRDD<ParseData> data = text.flatMap(new FlatMapFunction<String, ParseData>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Iterable<ParseData> call(String iLine) {
				return (Iterable<ParseData>) parseFile(iLine);
			}
		});
		//first part of parse Session by file
		JavaPairRDD<String, Session> lSession = data.mapToPair(ses -> constructMapOfSession(ses))
				.reduceByKey((ses1,ses2) -> constructSession(ses1,ses2));
		lSession.saveAsTextFile("SessionByFile");
		
		//Second part of parse Session by day
		JavaPairRDD<String, List<Session>> lListSession = data.mapToPair(ses -> constructMapOfListSession(ses))
				                                        .reduceByKey((ses1,ses2) ->constructListSession(ses1,ses2));
		lListSession.saveAsTextFile("SessionByDay");
	}
	private static List<Session> constructListSession(List<Session> iSes1, List<Session> iSes2){
		if(iSes1.size() == 0 && iSes2.size() == 0)return new ArrayList<>();
		if(iSes1.size() == 0 && iSes2.size() > 0)return iSes2;
		if(iSes2.size() == 0 && iSes1.size() > 0)return iSes1;
         Session lSes1 = iSes1.get(0);
         Session lSes2 = iSes2.get(0);
		Date lTimesTampConnect1 = lSes1.getmTimesTampConnect();
		Date lTimesTampConnect2 = lSes2.getmTimesTampConnect();
		Date lTimesTampConnect;
		Date lTimesTampDisConnect;
		long lDuration = 0;
		long lTime1MilSec = lTimesTampConnect1.getTime();
		long lTime2MilSec = lTimesTampConnect2.getTime();
		final long MILLISECONDS_PER_DAY = 1000 * 60 * 60 * 24; 
		long delta = lTime2MilSec - lTime1MilSec;
		long nbJour = delta / (MILLISECONDS_PER_DAY);
		if(nbJour < 1){
			String lListUrl = lSes1.getmListUrl()+","+lSes2.getmListUrl();
			String lListRequete = lSes1.getmListRequete()+","+lSes2.getmListRequete();
			String lIp = lSes1.getmIp();
			int lNumberOfPage = lSes1.getmNumberOfPage()+lSes2.getmNumberOfPage();
			lTimesTampConnect = lTimesTampConnect1;
			lTimesTampDisConnect = lTimesTampConnect2;
			lDuration = lTime2MilSec - lTime1MilSec;
			Session lSession = new Session(lIp, lTimesTampConnect, lTimesTampDisConnect,
					lDuration, lListUrl, lListRequete, lNumberOfPage);
			return Arrays.asList(lSession);
		}else {
			return Arrays.asList(lSes1,lSes2);

		}

	}
	private static Session constructSession(Session iSes1, Session iSes2){

		String lListUrl = iSes1.getmListUrl()+","+iSes2.getmListUrl();
		String lListRequete = iSes1.getmListRequete()+","+iSes2.getmListRequete();
		String lIp = iSes1.getmIp();
		int lNumberOfPage = iSes1.getmNumberOfPage()+iSes2.getmNumberOfPage();
		Date lTimesTampConnect1 = iSes1.getmTimesTampConnect();
		Date lTimesTampConnect2 = iSes2.getmTimesTampConnect();
		Date lTimesTampConnect;
		Date lTimesTampDisConnect;
		long lDuration = 0;
		long lTime1MilSec = lTimesTampConnect1.getTime();
		long lTime2MilSec = lTimesTampConnect2.getTime();
		if(lTimesTampConnect1.before(lTimesTampConnect2)){
			lTimesTampConnect = lTimesTampConnect1;
			lTimesTampDisConnect = lTimesTampConnect2;
			lDuration = lTime2MilSec - lTime1MilSec;
		}else {
			lTimesTampConnect = lTimesTampConnect2;
			lTimesTampDisConnect = lTimesTampConnect1;
			lDuration = lTime1MilSec - lTime2MilSec;
		}
		return new Session(lIp, lTimesTampConnect, lTimesTampDisConnect,
				lDuration, lListUrl, lListRequete, lNumberOfPage);

	}
	private static Tuple2<String,Session> constructMapOfSession(ParseData iData){
		String lUser = iData.getmIp();
		Date lTimesTampConnect = iData.getmTimesTamp();
		String lListRequete = iData.getmRequete();
		String lListUrl = iData.getmUrl();
		int lDuration = 0;
		int lNumberOfPage = 1;
		Session lSessionofUser = new Session(lUser, lTimesTampConnect, lTimesTampConnect,
				lDuration, lListUrl, lListRequete, lNumberOfPage);
		return new Tuple2<String,Session>(lUser,lSessionofUser);


	}
	private static Tuple2<String,List<Session>> constructMapOfListSession(ParseData iData){
		String lUser = iData.getmIp();
		Date lTimesTampConnect = iData.getmTimesTamp();
		String lListRequete = iData.getmRequete();
		String lListUrl = iData.getmUrl();
		int lDuration = 0;
		int lNumberOfPage = 1;
		Session lSessionofUser = new Session(lUser, lTimesTampConnect, lTimesTampConnect,
				lDuration, lListUrl, lListRequete, lNumberOfPage);
		return new Tuple2<String,List<Session>>(lUser,Arrays.asList(lSessionofUser));


	}

	private static Iterable<ParseData> parseFile(String iLine){
		String[] lSplitLine = iLine.split(" ");
		String lIp = lSplitLine[0];
		String lTimesLine = lSplitLine[3];
		String lTimes = lTimesLine.replace("[", "");
		lTimes = lTimes.replaceFirst(":", " ");
		lTimes = lTimes.replace("/", " ");
		DateFormat df = new SimpleDateFormat("dd MMM yyyy hh:mm:ss",Locale.ENGLISH);
		Date lTimesTamp = null;
		try {
			lTimesTamp = df.parse(lTimes);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String lRequete = lSplitLine[5];
		String lUrl = lSplitLine[6];
		ParseData lParse = new ParseData(lIp, lTimesTamp, lUrl, lRequete);
		return Arrays.asList(lParse);


	}
	private static Boolean filterAddressIp(String iLine){
		try {
			String[] lSplitLine = iLine.split(" ");
			String lIp = lSplitLine[0];
			if ( lIp == null || lIp.isEmpty() ) {
				return false;
			}

			String[] parts = lIp.split( "\\." );
			if ( parts.length != 4 ) {
				return false;
			}

			for ( String s : parts ) {
				int i = Integer.parseInt( s );
				if ( (i < 0) || (i > 255) ) {
					return false;
				}
			}
			if ( lIp.endsWith(".") ) {
				return false;
			}

			return true;
		} catch (NumberFormatException nfe) {
			return false;
		}
	}
	public static void main( String[] args )
	{
		if( args.length == 0 )
		{
			System.out.println( "Usage: log <file>" );
			System.exit( 0 );
		}

		processingLog( args[ 0 ] );
	}

}
