
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
	def main(args: Array[String]) {

    	StreamingExamples.setStreamingLogLevels()
	
	    // Spark Context initialization
		val appName = "TwitterPopularTags"	   
    	val conf = new SparkConf().setAppName(appName)
	    val sparkHomeDir = System.getenv("SPARK_HOME") 
    	val sc = new SparkContext(conf)
    
		// Initialize sqlContext so that we can write a parquet file that could be used to populate a Hive DB
    	val sqlContext = new org.apache.spark.sql.SQLContext(sc)
	    import sqlContext.createSchemaRDD

	  	case class Tweets(UserName: String, UserLocation: String, NumofFollowers: Int, NumofFriends: Int, NumofFavorites: Int)
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
	
		val usersCol = stream.map(user => "@" + user.getUser().getScreenName() + "," + user.getUser().getName()+ "," + user.getUser().getLocation() + "," + user.getUser().getLang() + "," + user.getUser().getFollowersCount() + "," + user.getUser().getFriendsCount() + "," + user.getUser().getFavouritesCount())
		usersCol.print()
		usersCol.foreachRDD( rdd => rdd.saveAsTextFile("tweets_table%s.parquet".format(java.lang.System.currentTimeMillis())))
		//script to combine output files: cat part-000* | awk -F "," '{print $1}' | sort -n | uniq -c > myout.txt
	    //usersCol.foreachRDD( rdd => rdd.saveAsParquetFile("tweets_table%s.parquet".format(java.lang.System.currentTimeMillis())))
		val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    	val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
        	             .map{case (topic, count) => (count, topic)}
            	         .transform(_.sortByKey(false))

	    /*val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
    	                 .map{case (topic, count) => (count, topic)}
        	             .transform(_.sortByKey(false))
		*/
    // Output Text file

	    val output = new PrintWriter(new FileWriter("myResultsFile", false))

	    topCounts60.foreachRDD(rdd => {

			val topList = rdd.take(10)
	    	println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
    	  	topList.foreach{case (count, tag) => logData(tag, count, output)}
	      //topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    	})
/*
    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(5)
      //println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })*/

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
