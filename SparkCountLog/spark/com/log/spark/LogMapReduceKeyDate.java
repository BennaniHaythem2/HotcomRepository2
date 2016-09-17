package com.log.spark;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/*
 * LogMapReduceKeyDate
 * c'est une class utilise pour compter les nombres des connctions 
 * par date et heurs pour chaque @IP
 * Méthode1:sformatDate pour formater la chaine de caractaire utiliser par le colonne s[1] date de connection
 * Méthode2: sCountDateAddIP c'est une méthode qui compte les date connection par date
 */

public class LogMapReduceKeyDate {

	// Méthode1 de formatage date
	public static Date sFormatDate(String x) throws ParseException {

		DateFormat formatter;
		Date date;
		formatter = new SimpleDateFormat("dd/MM/yyyy:hh:mm:ss");
		date = (Date) formatter.parse(x);
		String d = String.valueOf(date);
		// System.out.println("Today is " +d);

		return date;

	}

	// Méthode2 de compter Add IP
	public static void sCountDateAddIP(String fileName) {
		// Lire Le fichier Log
		SparkConf conf = new SparkConf().setAppName(
				"wikipedia-mapreduce-by-key").setMaster("local[16]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Recuper les colonnes S[0]= @IP te s[1]=Date
		JavaPairRDD<String, String> map1 = sc
				.textFile("path.txt")
				.map(line -> line.split(" "))
				.mapToPair(
						s -> new Tuple2<String, String>(s[0], s[1].replace("[",
								"")));
		// Map cle @IP et valeur Date
		JavaPairRDD<String, Date> trs = map1
				.mapToPair(new PairFunction<Tuple2<String, String>, String, Date>() {

					@Override
					public Tuple2<String, Date> call(Tuple2<String, String> arg0)
							throws Exception {
						// TODO Auto-generated method stub
						String x = arg0._1();

						String y = arg0._2();

						Date d = sFormatDate(y);

						return new Tuple2<String, Date>(x, d);
					}
				});

		// Map cle Date recuper d'@IP et valeur =1
		JavaPairRDD<Date, Integer> trs2 = trs
				.mapToPair(new PairFunction<Tuple2<String, Date>, Date, Integer>() {

					@Override
					public Tuple2<Date, Integer> call(Tuple2<String, Date> arg0)
							throws Exception {
						// TODO Auto-generated
						String x = arg0._1();

						Date y = arg0._2();

						System.out.println("Cle=" + x + "Value=" + y);

						return new Tuple2<Date, Integer>(y, 1);
					}

					// TODO Auto-generated method stub
				});
		// La somme de de nombre des dates des connections pour chaque @IP
		JavaPairRDD<Date, Integer> rdt = trs2
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer arg0, Integer arg1)
							throws Exception {
						// TODO Auto-generated method stub
						int x = arg0.intValue();
						int y = arg1.intValue();

						System.out.println("x=" + x + "y=" + y);
						return x + y;

					}
				});

		// Sauvegarder dans fichier OutPut Date
		rdt.saveAsTextFile("OutPutDate");

	}

}
