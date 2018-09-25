import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object TestScala {
  def main(args: Array[String]): Unit = {Logger.getLogger("org").setLevel(Level.OFF)
   Logger.getLogger("akka").setLevel(Level.OFF)
    print ("start")
    val conf = new SparkConf()
    conf.setAppName("WordFrequency")
    conf.setMaster("local[2]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    val inputFile = "product.csv.txt"

    // Load our input data.
    val input = sc.textFile(inputFile)
    // Split up into words.

    val words = input.flatMap(s => s.split(" "))
   // words.foreach(print)

  // print ("proverka")

    val wordsNumbers = words.map(s => (s, 1))
    // Transform into word and count.
    val counts = wordsNumbers.reduceByKey ((a, b) => a + b )
    // Save the word count back out to a text file, causing evaluation.
    val fullCount = words.count().toFloat

    val wordFrq = counts.map(s => (s._1, s._2, s._2.toFloat / fullCount))
    wordFrq.foreach(println)
  }
}

