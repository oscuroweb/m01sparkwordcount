package org.rhcalero.bigdata.module1.spark;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 
 * SparkWordCountWithAnonymousFunctions:
 * <p>
 * Count words from file using Spark with anonymous function
 * </p>
 * 
 * @author Hidalgo Calero, R.
 * @since Oct 15, 2016
 */
public class SparkWordCountWithAnonymousFunctions {

    /** Log instance. */
    private static Logger log = Logger.getLogger(SparkWordCountWithAnonymousFunctions.class.getName());

    /** White space constant. */
    private static final String WHITE_SPACE = " ";

    public static void main(String[] args) {

        // Validate arguments list. File name or directory is required
        if (args.length < 1) {
            log.fatal("[ERROR] RuntimeException: There must be at least one argument (a file name or directory)");
            throw new RuntimeException();
        }

        // Get initial time
        long initTime = System.currentTimeMillis();

        // STEP 1: Create a SparkConf object
        SparkConf conf = new SparkConf();

        // STEP 2: Create a Java Spark Context
        JavaSparkContext context = new JavaSparkContext(conf);

        // STEP 3: Get lines from file(s) and put it in a Java RDD instance
        JavaRDD<String> lines = context.textFile(args[0]);
        log.debug("[DEBUG] STEP 3: Get lines from file(s) and put it in a Java RDD instance");

        // STEP 4: Remove empty lines
        JavaRDD<String> filterdLines = lines.filter(new Function<String, Boolean>() {
            /** Generated serial version */
            private static final long serialVersionUID = -6153917332118394342L;

            @Override
            public Boolean call(String line) throws Exception {
                return !line.isEmpty();
            }
        });
        log.debug("[DEBUG] STEP 4: Remove empty lines");

        // STEP 5: Get words from Java RDD lines
        JavaRDD<String> words = filterdLines.flatMap(new FlatMapFunction<String, String>() {
            /** Generated serial version */
            private static final long serialVersionUID = 6910407091578945797L;

            @Override
            public Iterator<String> call(String line) throws Exception {
                String[] wordsArray = line.split(WHITE_SPACE);
                Iterator<String> wordsIt = Arrays.asList(wordsArray).iterator();
                return wordsIt;
            }

        });
        log.debug("[DEBUG] STEP 5: Get words from Java RDD lines");

        // STEP 6: Get a Java Pair RDD where key is a word and value is 1
        JavaPairRDD<String, Integer> wordPair = words.mapToPair(new PairFunction<String, String, Integer>() {
            /** Generated serial version */
            private static final long serialVersionUID = -744560274505893811L;

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                // Transform word to lower case string & Remove . to the word & Remove , to the word
                String formatterWord = word.toLowerCase().replaceAll("\\.", StringUtils.EMPTY)
                        .replaceAll("\\,", StringUtils.EMPTY);
                // Return the tuple
                return new Tuple2<String, Integer>(formatterWord.trim(), 1);
            }
        });
        log.debug("STEP 6: Get a Java Pair RDD where key is a word and value is 1");

        // STEP 7: Reduced word pair with the sum of the values
        JavaPairRDD<String, Integer> reducedWordPair = wordPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            /** Generated serial version */
            private static final long serialVersionUID = -1291032878715402897L;

            @Override
            public Integer call(Integer value1, Integer value2) throws Exception {
                return value1 + value2;
            }
        });
        log.debug("STEP 7: Reduced word pair with the sum of the values");

        // STEP 8: Transform reduced pair (where key is word and value is count) to a new one where key is count and
        // value is word
        JavaPairRDD<Integer, String> wordCountReducedPair = reducedWordPair
                .mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    /** Generated serial version */
                    private static final long serialVersionUID = 9148190567240100181L;

                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> wordCountTuple) throws Exception {
                        return new Tuple2<Integer, String>(wordCountTuple._2(), wordCountTuple._1());
                    }
                });
        log.debug("STEP 8: Transform reduced pair (where key is word and value is count) to a new one where key is count and value is word");

        // STEP 9: Sort by key reduced word pair in descent mode
        JavaPairRDD<Integer, String> sortedReducedWordPair = wordCountReducedPair.sortByKey(false);
        log.debug("STEP 9: Sort by key reduced word pair");

        // STEP 10: Parse Java RDD pair to a list
        List<Tuple2<Integer, String>> wordsCountList = sortedReducedWordPair.collect();
        log.debug("STEP 10: Parse Java RDD pair to a list");

        // Get computing time
        long computingTime = System.currentTimeMillis() - initTime;

        // STEP 11: Print the result
        for (Tuple2<Integer, String> tuple : wordsCountList) {
            StringBuffer tupleStr = new StringBuffer();
            tupleStr.append(tuple._2()).append(": ").append(tuple._1());
            System.out.println(tupleStr.toString());
        }
        System.out.println("Computing time: " + computingTime);
        System.out.println("Total words: " + wordsCountList.size());

        // STEP 12: Stop the spark context
        context.stop();
        context.close();
    }
}
