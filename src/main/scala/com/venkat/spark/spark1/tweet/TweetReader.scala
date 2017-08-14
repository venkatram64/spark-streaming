package com.venkat.spark.spark1.tweet

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.io.Source

/**
  * Created by venkatram.veerareddy on 8/14/2017.
  */

object TweetReader extends App{

  TwitterConnection.twitterConnection()
  val sc = new StreamingContext("local[*]", "TweetsProcessor", Seconds(2))


  val tweets = TwitterUtils.createStream(sc, None)

  val statuses = tweets.map(status => status.getText)

  val tweetWords = statuses.flatMap(tweetText => tweetText.split(" "))

  val hashtags = tweetWords.filter(word => word.startsWith(("#")))

  val hastagKeyValues = hashtags.map(hashtag => (hashtag, 1))

  val hashtagCounts = hastagKeyValues.reduceByKeyAndWindow((x,y) => x + y, (x,y) => x - y, Seconds(300),Seconds(2))

  val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, false))

  sortedResults.print()

  sc.checkpoint("D:/xyz/checkpoint/")
  sc.start()
  sc.awaitTermination()


}

object TwitterConnection{
  def twitterConnection(): Unit= {
    for(line <- Source.fromFile("twitter.txt").getLines()){
      val fields = line.split(" ")
      if(fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }
}
