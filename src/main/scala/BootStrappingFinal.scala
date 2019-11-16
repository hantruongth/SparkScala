import java.io.{File, PrintWriter}

import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Map


case class Population(category: String, mpg: String)

object BootStrappingFinal extends App {

  def KEY = "category"

  def VALUE = "mpg"

  def N = 10


  def bootstrapping(bootstrapsample: DataFrame, percentage: Float): DataFrame = {
    val startTime = java.lang.System.currentTimeMillis()
    import org.apache.spark.sql.functions._
    var tempDF = bootstrapsample.sample(true, 1f).groupBy(KEY).agg(format_number(avg(VALUE), 2) as "Mean", format_number(variance(VALUE), 2) as "Variance")
    println("Step 5: resample 1")
    tempDF.show()
    for (i <- 2 to N) {
      // Step 5a
      val resample = bootstrapsample.sample(true, percentage);

      // Step 5b
      val df = resample.groupBy(KEY).agg(format_number(avg(VALUE), 2) as "Mean", format_number(variance(VALUE), 2) as "Variance")

      // Step 5c
      tempDF = df.union(tempDF)
      tempDF = tempDF.groupBy(KEY).agg(format_number(sum("Mean"), 2) as "Mean", format_number(sum("Variance"), 2) as "Variance")
      //          if (i <= 3 || i >= N - 1) {
      println("Step 5: resample " + i)
      df.show()
      tempDF.show()
      //          }
    }
    println("Step 6:")
    val df = tempDF.groupBy(KEY).agg(format_number(sum("Mean") / N, 2) as "Mean", format_number(sum("Variance") / N, 2) as "Variance").cache()
    df.show()
    println("Solution completed in " + (java.lang.System.currentTimeMillis() - startTime) + " ms")
    return df
  }

  def dataframe2map(df: DataFrame): Map[String, (Float, Float)] = {
    val map: Map[String, (Float, Float)] = Map()
    val rows = df.collectAsList();
    for (j <- 0 to rows.size() - 1) {
      val row = rows.get(j)

      var valx = 0f
      if (row.getString(2) != null)
        valx = row.getString(2).toFloat
      else
        valx = 0f

      map.put(row.getString(0), (row.getString(1).toFloat, valx))
    }
    return map
  }

  def calc(orgDF: DataFrame, percentage: Float, actualMap: Map[String, (Float, Float)], meanWriter: PrintWriter, varianceWriter: PrintWriter) {
    print("\nCalculating percentage=" + percentage + ", N=" + N)
    val bootstrapsample = orgDF.sample(false, percentage)
    println(", " + bootstrapsample.count() + " samples")
    val df = bootstrapping(bootstrapsample, percentage)
    val estimateMap = dataframe2map(df)
    meanWriter.println()
    meanWriter.print(percentage)
    varianceWriter.println()
    varianceWriter.print(percentage)
    for ((key, actual) <- actualMap) {
      if (estimateMap.contains(key)) {
        // abs(actual – estimate)*100/actual
        val estimate = estimateMap(key)
        val diffMean = (math.abs(actual._1 - estimate._1) * 100 / actual._1)
        val diffVariance = (math.abs(actual._2 - estimate._2) * 100 / actual._2)
        meanWriter.print("," + diffMean)
        varianceWriter.print("," + diffVariance)
        println(key + "\t" + diffMean + "\t" + diffVariance)
      } else {
        meanWriter.print(",")
        varianceWriter.print(",")
        println(key)
      }
    }
    meanWriter.flush();
    varianceWriter.flush();
  }


  override def main(args: Array[String]): Unit = {

    def FILENAME = "mtcars.csv"

    def KEY_COL = 2

    def VALUE_COL = 1

    val conf = new SparkConf().setAppName("Spark and SparkSql").setMaster("local")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val csv = sc.textFile(FILENAME)
    // Create a Spark DataFrame
    val headerAndRows = csv.map(line => line.split(",").map(_.trim))
    val header = headerAndRows.first
    val orgData = headerAndRows.filter(_ (0) != header(0))
    println(orgData.count() + " rows")

    println("Step 2:")
    val orgDF = orgData
      .map(p => Population(p(KEY_COL), p(VALUE_COL)))
      .toDF
    orgDF.printSchema
    import org.apache.spark.sql.functions._

    println("Step 3: Actual Data Frame")
    val actualDF = orgDF.groupBy(KEY).agg(format_number(avg(VALUE), 2) as "Mean", format_number(variance(VALUE), 2) as "Variance").cache()
    actualDF.show()

    println("Step 4: BootStrapping")
    val bootstrapsample = orgDF.sample(false, 0.25).cache()
    bootstrapsample.show(10)

    bootstrapping(bootstrapsample, 1f)

    //OPTIONAL
    //These are fun steps for those who want to explore and learn more.

    val actualMap = dataframe2map(actualDF)
    println(actualMap)

    val meanWriter = new PrintWriter(new File("mean.csv"))
    val varianceWriter = new PrintWriter(new File("variance.csv"))
    meanWriter.print("Percentage")
    varianceWriter.print("Percentage")
    for ((k, v) <- actualMap) {
      meanWriter.print("," + k)
      varianceWriter.print("," + k)
    }

    calc(orgDF, 0.05f, actualMap, meanWriter, varianceWriter) // 5%
    calc(orgDF, 0.25f, actualMap, meanWriter, varianceWriter) // 25%
    calc(orgDF, 0.5f, actualMap, meanWriter, varianceWriter) // 50%
    calc(orgDF, 0.75f, actualMap, meanWriter, varianceWriter) // 75%

    meanWriter.close()
    varianceWriter.close()

  }

}
