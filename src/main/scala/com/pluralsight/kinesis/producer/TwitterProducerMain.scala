package com.pluralsight.kinesis.producer

import com.amazonaws.services.kinesis.producer._
import com.google.common.collect.Iterables
import com.google.common.util.concurrent.FutureCallback
import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import twitter4j._
import twitter4j.conf.ConfigurationBuilder
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets


object TwitterProducerMain {

  def main(args: Array[String]): Unit = {
    // create twitter stream
    val twitterStream:TwitterStream = createTwitterStream();

    //create twitter listerner
    twitterStream.addListener(createListener());

    //twitter sample data
    twitterStream.sample();

  }

  private def createTwitterStream():TwitterStream ={
    //create a Twitter stream
    //step 1 create configurations and pass the access keys
    val cb = new ConfigurationBuilder
    cb.setOAuthConsumerKey("WuJr0R44DKo8I4jFSorDA7RB4")
      .setOAuthConsumerSecret("BqS81nlav3mXEpqb3RcHivOBOMWfjWiSOdt4RXBzfDpfztjBjN")
      .setOAuthAccessToken("148920981-WiFVmOPETfHf0rxzPf8PPRqINka9kPCKp5O9RuAq")
      .setOAuthAccessTokenSecret("FbBUcX0OXymgb2DRiDMZkb9xNnUCl9ofiC2zf0gJuPWSK")

    // create new instance of twitter stream
    return new TwitterStreamFactory(cb.build).getInstance
  }

  private def createListener():RawStreamListener = {
    return new TwitterProducerMain.TweetsStatusListener
  }

  private class TweetsStatusListener extends RawStreamListener {
    override def onMessage(tweetJson: String): Unit = {
      try {
        val status = TwitterObjectFactory.createStatus(tweetJson)
        if (status.getUser != null) {
          System.out.println(tweetJson)
          System.out.println(status.getUser)
          System.out.println(status.getText)
        }
      } catch {
        case e: TwitterException =>
          e.printStackTrace()
      }
    }

    override def onException(e: Exception): Unit = {
      e.printStackTrace()
    }
  }




}
