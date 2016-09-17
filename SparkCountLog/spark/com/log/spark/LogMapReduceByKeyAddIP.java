package com.log.spark;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Currency;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.spark.SparkConf;

import akka.serialization.JavaSerializer.CurrentSystem;

import com.google.common.collect.Iterables;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;

import com.google.common.base.Optional;

import scala.Tuple2;
import scala.Tuple3;

/*
 * LogMapReduceKey 
 * Log MapReduceKeyAddIP est une class utilisé pour compter le nombre des actions sont utilisé par les @IP
 * Utiliser la méthode Count Les Actions
 */

public class LogMapReduceByKeyAddIP {

	// Méthode Count Actions
	public static void sCountLesActions( String filename ) {

		// Lire Le fichier log et tiré les données des 2 colonnes @IP et Les
		// actions correspondants
		SparkConf conf = new SparkConf().setAppName(
				"wikipedia-mapreduce-by-key").setMaster("local[16]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Créer une map avec @IP comme cle s[0] et comme value c'est la colonne
		// action utilisé(Get ou Post) s[3]
		JavaPairRDD<String, String> map = sc.textFile(filename)
				.map(line -> line.split(" "))
				.mapToPair(s -> new Tuple2<String, String>(s[0], s[3]));

		
		JavaPairRDD<String, String> reduce = map
				.reduceByKey(new Function2<String, String, String>() {
					@Override
					public String call(final String value0, final String value1) {
						return value0 + value1;
					}
				});
		// Créer une map contient la somme des actions de meme @IP	
		JavaPairRDD<String, String> trs = reduce
				.mapToPair(new PairFunction<Tuple2<String, String>, String,String>() {

					@Override
					public Tuple2<String, String> call(Tuple2<String, String> arg0)
							throws Exception {
						// TODO Auto-generated method stub
						String x = arg0._1();

						String y = arg0._2();


						return new Tuple2<String, String>(x, y);
					}
				});
//
//		// Regrouper les memes actions de meme cle @IP
//		JavaPairRDD<String, Iterable<String>> gby1 = trs.groupByKey().cache();
		// Créer une map contient l'ensemble des actions(Get et Post) de meme
		// @IP et leurs valeurs et 1
		JavaPairRDD<String, Integer> r =trs
				.mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {

					@Override
					public Tuple2<String, Integer> call(
							Tuple2<String, String> arg0) throws Exception {
						// TODO Auto-generated
						String x = arg0._1();

						String y = arg0._2();

						System.out.println("Cle=" + x + "Value=" + y
								+ "predectuion=" + 1);

						return new Tuple2<String, Integer>(y, 1);
					}
				});
		// La somme de nombre des actions de meme cle @IP
		JavaPairRDD<String, Integer> r2 = r
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer arg0, Integer arg1)
							throws Exception {
						// TODO Auto-generated method stub

						return arg0 + arg1;

					}
				});

		//Sauvegarder dans un fichier outputAction1 pour les nombres des valeurs des actions utilisé pour les @IP 
		r2.saveAsTextFile("outputAction1");
	}
 
	
}
