package src;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


/**
* This class uses Dataset APIs of spark to count number of articles per month
* The year-month is obtained as a dataset of String
* */

public class NewsMonth {

	public static void main(String[] args) {
		
		//Input dir - should contain all input json files
		String inputPath = "./newsdata/";
		
		//Output dir - this directory will be created by spark. Delete this directory between each run
		String outputPath = "./output/";
		
		SparkSession sparkSession = SparkSession.builder()
				.appName("Month wise news articles")		//Name of application
				.master("local")								//Run the application on local node
				.config("spark.sql.shuffle.partitions","2")		//Number of partitions
				.getOrCreate();
		
		//Read multi-line JSON from input files to dataset
		Dataset<Row> inputDataset = sparkSession.read().option("multiLine", true).json(inputPath);   
		
		
		// Apply the map function to extract the year-month
//		Dataset<String> yearMonthDataset=inputDataset.map(new MapFunction<Row,String>(){
//			public String call(Row row) throws Exception {
//				// The first 7 characters of date_published gives the year-month 
//				String yearMonthPublished=((String)row.getAs("date_published")).substring(0, 7);
//				return yearMonthPublished;	  
//			}
//			
//		}, Encoders.STRING());
		
		//Alternative way use flatmap using lambda function
		Dataset<String> yearMonthDataset=inputDataset.map(
				row-> {return ((String)row.getAs("date_published")).substring(0, 7);}, 
						Encoders.STRING());
		
		
		
		//The column name of the dataset for string encoding is value
		// Group by the desired column(s) and take count. groupBy() takes 1 or more parameters
		//Rename the result column as year_month_count
		Dataset<Row> count=yearMonthDataset.groupBy("value").count().as("year_month_count");
		
		
		//Outputs the dataset to the standard output
		//count.show();
		
		
		//Ouputs the result to a file
		count.toJavaRDD().saveAsTextFile(outputPath);	
		
	}
	
}