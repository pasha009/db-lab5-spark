package src;


import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

//Set appropriate package name

import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
* This class uses Dataset APIs of spark to count number of articles per month
* The year-month is obtained as a dataset of Row
* */

public class NewsMonthRow {

	public static void main(String[] args) {
		
		//Input dir - should contain all input json files
		String inputPath = "./newsdata/";
		
		//Output dir - this directory will be created by spark. Delete this directory between each run
		String outputPath = "./output/";   
		
		SparkSession sparkSession = SparkSession.builder()
				.appName("Word count")		//Name of application
				.master("local")								//Run the application on local node
				.config("spark.sql.shuffle.partitions","2")		//Number of partitions
				.getOrCreate();
		
		Dataset<Row> inputDataset=sparkSession.read().option("multiLine", true).json(inputPath);
		
		StructType structType = new StructType();
	    structType = structType.add("year-month", DataTypes.StringType, false); // false => not nullable
	    structType = structType.add("word", DataTypes.StringType, false); // false => not nullable
	    
	    ExpressionEncoder<Row> dateRowEncoder = RowEncoder.apply(structType);
	    
	    Dataset<Row> yearMonthDataset=inputDataset.flatMap(
	    		(FlatMapFunction<Row, Row>) row -> {
	    			String yearMonthPublished = ((String)row.getAs("date_published")).substring(0, 7);
	    			String article = (String) row.getAs("article_body");
	    			article = article.toLowerCase().replaceAll("[^A-Za-z]", " ");  //Remove all punctuation and convert to lower case
	    			article = article.replaceAll("( )+", " ");   //Remove all double spaces
	    			article = article.trim(); 
					List<String> wordList = Arrays.asList(article.split(" ")); //Get words
					List<Row> rows = Arrays.asList(new Row[wordList.size()]);
					for(int i = 0; i < wordList.size(); i++) {
						rows.set(i, RowFactory.create(yearMonthPublished, wordList.get(i)));
					}
					return rows.iterator();
	    		}, dateRowEncoder);
		
	    Dataset<Row> count = yearMonthDataset.groupBy("year-month", "word").count().as("year_month_word_count");  
	    
	    count.toJavaRDD().saveAsTextFile(outputPath);	

	}
	
}