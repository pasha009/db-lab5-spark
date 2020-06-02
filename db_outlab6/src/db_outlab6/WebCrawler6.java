package db_outlab6;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class WebCrawler6 {
	
	public static void main(String[] args) {
		
		String base_url = "https://www.cse.iitb.ac.in/"; // should end in a slash
		String outputPath="/home/pasha/Desktop/sem6/db_lab_CS387/lab5/outlab/outputs/output_P2";
		int max_hops = 5;
		
		SparkSession sparkSession = SparkSession.builder()
				.appName("Web Crawler")							// Name of application
				.master("local")								// Run the application on local node
				.config("spark.sql.shuffle.partitions","1")		// Number of partitions
				.config("spark.sql.broadcastTimeout", "1200")	// Timeout
				.getOrCreate();
		
		StructType structType = new StructType();
		structType = structType.add("abs_url", DataTypes.StringType, false); 	// false => not nullable
		structType = structType.add("title", DataTypes.StringType, false); 		// false => not nullable
		structType = structType.add("crawled", DataTypes.BooleanType, false); 	// false => not nullable
		
		ExpressionEncoder<Row> webCrawlerEncoder = RowEncoder.apply(structType);
				
		
		Dataset<Row> cumulative = sparkSession.createDataset(
				Arrays.asList(RowFactory.create(base_url, "", false)),
				webCrawlerEncoder);
		
		Dataset<Row> to_crawl = null;
		
		while(max_hops > 0) {
			
			// filter all which are not crawled
			to_crawl = cumulative.filter(cumulative.col("crawled").$eq$eq$eq(false)); 
			
			// exit if no urls to crawl
			long new_urls = to_crawl.count();
			if(new_urls == 0) break;
			
			// add new urls obtained and remove duplicates
			to_crawl = to_crawl.flatMap((FlatMapFunction<Row, Row>) row -> {
				
				List<Row> new_rows = new ArrayList<Row>();
				String abs_url = (String) row.getAs("abs_url");
				String l;

				try {

		    		Document doc = Jsoup.connect(abs_url).get();
		    		Elements links = doc.select("a");
		    		
					for (Element link : links) {
						l = sanitize(link.absUrl("href"));
						if (!l.startsWith(base_url)) continue;
						new_rows.add(RowFactory.create(l, "", false));
					}
					
					Elements titles = doc.select("div.pad");
					if (titles.isEmpty()) 
						new_rows.add(RowFactory.create(abs_url, "", true));
					else
						new_rows.add(RowFactory.create(abs_url, titles.first().text(), true));
					
				} catch (HttpStatusException h) {
					new_rows.add(RowFactory.create(abs_url, "", true));
				} catch (IOException i) {
					new_rows.add(RowFactory.create(abs_url, "", true));
				}
				
				return new_rows.iterator();

			}, webCrawlerEncoder).distinct();
			
			// group urls with same url keeping title and crawled status
			cumulative = cumulative
					.union(to_crawl)
					.groupBy("abs_url")
					.agg(functions.max("title").as("title"), functions.max("crawled").as("crawled"))
					.cache();
			
			max_hops--;
			System.out.println(max_hops);
		}
		
		cumulative.select("abs_url", "title").toJavaRDD().saveAsTextFile(outputPath);
	}
	
	private static String sanitize(String tem) {
		tem = tem.replaceAll("#(.*)", "");
		if(!tem.contains("/page")) return "donkey";			// return an unacceptable string
		return tem;
	}
}
