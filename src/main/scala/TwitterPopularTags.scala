
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j._
import StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.io._

/**
 * Calculates popular hashtags (topics) over sliding 10 and 60 second windows from a Twitter
 * stream. The stream is instantiated with credentials and optionally filters supplied by the
 * command line arguments.
 *
 */
object TwitterPopularTags {


case class Tweet (
		/*id: String,
		//reply_status_id: Int,
		//reply_user_id: Int,
		retweet_count: Int,*/
		text: String,
		/*//latitude: Float,
		  //longitude: Float,
		  source: String,
		  user_id: Int,
		  */
		user_name: String,
		user_screen_name: String,
		user_created_at: String,
		user_followers: String,
		user_favorites: String,
		user_language: String,
		user_location: String,
		user_timezone: String,
		created_at: String
		/*,
		created_at_year: Int,
		created_at_month: Int,
		created_at_day: Int,
		created_at_hour: Int,
		created_at_minute: Int,
		created_at_second: Int*/)


	def main(args: Array[String]) {

    	StreamingExamples.setStreamingLogLevels()
	
	    // Spark Context initialization
		val appName = "TwitterPopularTags"	   
    	val conf = new SparkConf().setAppName(appName)
	    val sparkHomeDir = System.getenv("SPARK_HOME") 
    	val sc = new SparkContext(conf)
    
		// [NOT USED] Initialize sqlContext so that we can write a parquet file that could be used to populate a Hive DB
    	val sqlContext = new org.apache.spark.sql.SQLContext(sc)
	    import sqlContext.createSchemaRDD

		//val theTweets: RDD[Tweets] = ...
		//theTweets.saveAsParquetFile("myTweets.parquet")



		// Set filter only if you want to capture certain tweets
		//val filters : Array[String] = Array("ISIS","obama")

	    System.setProperty("twitter4j.oauth.consumerKey", "hGctR7W3lvvR6XzCUIDouX4Zz")
    	System.setProperty("twitter4j.oauth.consumerSecret", "qQd0n2BCeSlVw2VGMMSpXlasppVDZtKy5dHFOaRCmOczYrC1YB")
	    System.setProperty("twitter4j.oauth.accessToken", "68665125-vVsXx61LmCRqg2opqZCcTQxmoYknPMl2W0gx5qQpT")
    	System.setProperty("twitter4j.oauth.accessTokenSecret", "MEXA5mRhHx8qqCTZ0vDs1OdHCAHAsTbqB5ZcNclNZWav4")

	    val ssc = new StreamingContext( sc, Seconds(60)) 
		val stream = TwitterUtils.createStream(ssc, None, Nil)

		val hiveDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.0")
		val year = new java.text.SimpleDateFormat("yyyy")
		val month = new java.text.SimpleDateFormat("MM")
		val day = new java.text.SimpleDateFormat("dd")
		val hour = new java.text.SimpleDateFormat("HH")
		val minute = new java.text.SimpleDateFormat("mm")
		val second = new java.text.SimpleDateFormat("ss")
		
		// ref: https://github.com/pwendell/spark-twitter-collection/blob/master/TwitterCollector.scala
	    // A list of Tweet fields we want along with Hive column names and data types
		val fields: Seq[(Status => Any, String, String)] = Seq(
		(s => s.getId, "id", "BIGINT"),
		(s => s.getInReplyToStatusId, "reply_status_id", "BIGINT"),
		(s => s.getInReplyToUserId, "reply_user_id", "BIGINT"),
		(s => s.getRetweetCount, "retweet_count", "INT"),
		(s => s.getText, "text", "STRING"),
		(s => Option(s.getGeoLocation).map(_.getLatitude()).getOrElse(""), "latitude", "FLOAT"),
		(s => Option(s.getGeoLocation).map(_.getLongitude()).getOrElse(""), "longitude", "FLOAT"),
		(s => s.getSource, "source", "STRING"),
		(s => s.getUser.getId, "user_id", "INT"),
		(s => s.getUser.getName, "user_name", "STRING"),
		(s => s.getUser.getScreenName, "user_screen_name", "STRING"),
		(s => hiveDateFormat.format(s.getUser.getCreatedAt), "user_created_at", "TIMESTAMP"),
		(s => s.getUser.getFollowersCount, "user_followers", "BIGINT"),
		(s => s.getUser.getFavouritesCount, "user_favorites", "BIGINT"),
		(s => s.getUser.getLang, "user_language", "STRING"),
		(s => s.getUser.getLocation, "user_location", "STRING"),
		(s => s.getUser.getTimeZone, "user_timezone", "STRING"),
		// Break out date fields for partitioning
		(s => hiveDateFormat.format(s.getCreatedAt), "created_at", "TIMESTAMP"),
		(s => year.format(s.getCreatedAt), "created_at_year", "INT"),
		(s => month.format(s.getCreatedAt), "created_at_month", "INT"),
		(s => day.format(s.getCreatedAt), "created_at_day", "INT"),
		(s => hour.format(s.getCreatedAt), "created_at_hour", "INT"),
		(s => minute.format(s.getCreatedAt), "created_at_minute", "INT"),
		(s => second.format(s.getCreatedAt), "created_at_second", "INT")
		)
		// For making a table later, print out the schema
		val tableSchema = fields.map{case (f, name, hiveType) => "%s %s".format(name, hiveType)}.mkString("(", ", ", ")")
		println("Table schema for Hive is: %s".format(tableSchema))

		def formatStatus(s: Status): String = {
			def safeValue(a: Any) = Option(a)
					.map(_.toString)
					.map(_.replace("\t", ""))
					.map(_.replace("\"", ""))
					.map(_.replace("\n", ""))
					.map(_.replaceAll("[\\p{C}]","")) // Control characters
					.getOrElse("")
				fields.map{case (f, name, hiveType) => f(s)}
				.map(f => safeValue(f))
				.mkString("\t")
		}		
		val userDetails = stream.map(s => formatStatus(s))

		 // Coalesce each batch into 1 partition
		val coalesced = userDetails.transform(rdd => rdd.coalesce(1))
		coalesced.foreachRDD( (rdd, time) => rdd.saveAsTextFile("tweets_%s".format(time)))
		
		/*coalesced.foreachRDD( rdd => {
							val mapCol = rdd.map(_.split("\t")).map(t => Tweet( t(0), t(1), t (2), t(3), t(4), t(5), t(6), t(7), t(8), t(9))) 
							mapCol.registerAsTable("tweet")
							val fromCountries = sqlContext.sql("SELECT  user_location, COUNT(*) from tweet GROUP BY user_location")
		        			//val fromCountries = sqlContext.sql("SELECT user_name, user_location from tweet")
		        			fromCountries.map(col => "name: " + col(0) + " loc: " + col(1)  ).collect().foreach(println)
							}
						)
		*/

		//script to combine output files: cat part-000* | awk -F "," '{print $1}' | sort -n | uniq -c > myout.txt
	    //usersCol.foreachRDD( rdd => rdd.saveAsParquetFile("tweets_table%s.parquet".format(java.lang.System.currentTimeMillis())))
		val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    	val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
        	             .map{case (topic, count) => (count, topic)}
            	         .transform(_.sortByKey(false))


	    val output = new PrintWriter(new FileWriter("myResultsFile", false))

	    topCounts60.foreachRDD(rdd => {

			val topList = rdd.take(10)
	    	println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
    	  	topList.foreach{case (count, tag) => logData(tag, count, output)}
    	})

    ssc.start()
    ssc.awaitTermination()
  }

	def logData(tag: String, count: Int, output: PrintWriter)
	{
	
      	println("%s (%s tweets)".format(tag, count))
    	val myoutput = new PrintWriter(new FileWriter("myResultsFile", true ))
		myoutput.write("%s %s\n".format(tag, count))
		myoutput.close()


	}
}
