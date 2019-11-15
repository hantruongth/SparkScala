import java.io.PrintWriter

import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Map

case class Data(rank: String, year: String)

object BootStrapping extends App {

  def KEY = "rank"

  def VALUE = "year"

  def N = 100

  override def main(args: Array[String]) {

    def FILENAME = "Salaries.csv"

    def KEY_COL = 1

    def VALUE_COL = 4


    val conf = new SparkConf().setAppName("Spark and SparkSql").setMaster("local")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    // Load a cvs file
    val csv = sc.textFile(FILENAME)
    // Create a Spark DataFrame
    val headerAndRows = csv.map(line => line.split(",").map(_.trim))
    val header = headerAndRows.first
    val orgData = headerAndRows.filter(_ (0) != header(0))
    println(orgData.count() + " rows")

    println("Step 2:")
    val orgDF = orgData
      .map(p => Data(p(KEY_COL), p(VALUE_COL)))
      .toDF
    orgDF.printSchema
    import org.apache.spark.sql.functions._

    println("Step 3:")
    val actualDF = orgDF.groupBy(KEY).agg(format_number(avg(VALUE), 2) as "Mean", format_number(variance(VALUE), 2) as "Variance").cache()
    actualDF.show()

    println("Step 4: bootstrapsample")
    val bootstrapsample = orgDF.sample(false, 0.25).cache()
    bootstrapsample.show(10)

    bootstrapping1(bootstrapsample)
    //bootstrapping2(bootstrapsample)
    //bootstrapping3(bootstrapsample)
    //bootstrapping4(bootstrapsample)

    //    val actualMap = dataframe2map(actualDF)
    //    //    println(actualMap)
    //    val meanWriter = new PrintWriter(new File("mean.csv"))
    //    val varianceWriter = new PrintWriter(new File("variance.csv"))
    //    meanWriter.print("Percentage")
    //    varianceWriter.print("Percentage")
    //    for ((k, v) <- actualMap) {
    //      meanWriter.print("," + k)
    //      varianceWriter.print("," + k)
    //    }
    //
    //    calc(orgDF, 0.05f, actualMap, meanWriter, varianceWriter)
    //    calc(orgDF, 0.25f, actualMap, meanWriter, varianceWriter)
    //    calc(orgDF, 0.5f, actualMap, meanWriter, varianceWriter)
    //    calc(orgDF, 0.75f, actualMap, meanWriter, varianceWriter)
    //
    //    meanWriter.close()
    //    varianceWriter.close()
  }

  def bootstrapping1(bootstrapsample: DataFrame): DataFrame = {
    val startTime = java.lang.System.currentTimeMillis()
    var resample = bootstrapsample.sample(true, 1)
    println("Step 5: resample 1")
    for (i <- 2 to N) {
      if (i <= 3 || i % 50 == 0 || i >= N - 2) {
        println("Step 5: resample " + i)
      }
      resample = resample.union(bootstrapsample.sample(true, 1))
    }

    println("Step 6:")
    import org.apache.spark.sql.functions._
    val df = resample.groupBy(KEY).agg(format_number(avg(VALUE), 2) as "Mean", format_number(variance(VALUE), 2) as "Variance").cache()
    df.show()
    println("Solution 1 completed in " + (java.lang.System.currentTimeMillis() - startTime) + " ms")
    return df
  }

  def bootstrapping2(bootstrapsample: DataFrame): DataFrame = {
    val startTime = java.lang.System.currentTimeMillis()
    import org.apache.spark.sql.functions._
    var unionDF = bootstrapsample.sample(true, 1f).groupBy(KEY).agg(format_number(avg(VALUE), 2) as "Mean", format_number(variance(VALUE), 2) as "Variance")
    println("Step 5: resample 1")
    unionDF.show()
    for (i <- 2 to N) {
      // Step 5a
      val resample = bootstrapsample.sample(true, 1f);

      // Step 5b
      val df = resample.groupBy(KEY).agg(format_number(avg(VALUE), 2) as "Mean", format_number(variance(VALUE), 2) as "Variance")

      // Step 5c
      unionDF = unionDF.union(df).cache()
      if (i <= 3 || i >= N - 1) {
        println("Step 5: resample " + i)
        df.show()
        unionDF.show()
      }
    }
    println("Step 6:")
    val df = unionDF.groupBy(KEY).agg(format_number(avg("Mean"), 2) as "Mean", format_number(avg("Variance"), 2) as "Variance").cache()
    df.show()
    println("Solution 2 completed in " + (java.lang.System.currentTimeMillis() - startTime) + " ms")
    return df
  }

  def bootstrapping3(bootstrapsample: DataFrame): DataFrame = {
    val startTime = java.lang.System.currentTimeMillis()
    import org.apache.spark.sql.functions._
    var tempDF = bootstrapsample.sample(true, 1f).groupBy(KEY).agg(format_number(avg(VALUE), 2) as "Mean", format_number(variance(VALUE), 2) as "Variance")
    println("Step 5: resample 1")
    tempDF.show()
    for (i <- 2 to N) {
      // Step 5a
      val resample = bootstrapsample.sample(true, 1f);

      // Step 5b
      val df = resample.groupBy(KEY).agg(format_number(avg(VALUE), 2) as "Mean", format_number(variance(VALUE), 2) as "Variance")

      // Step 5c
      tempDF = df.union(tempDF)
      tempDF = tempDF.groupBy(KEY).agg(format_number(sum("Mean"), 2) as "Mean", format_number(sum("Variance"), 2) as "Variance")
      //          if (i <= 3 || i >= N - 1) {
      println("Step 5: resample " + i)
      //            df.show()
      //                tempDF.show()
      //          }
    }
    println("Step 6:")
    val df = tempDF.groupBy(KEY).agg(format_number(sum("Mean") / N, 2) as "Mean", format_number(sum("Variance") / N, 2) as "Variance").cache()
    df.show()
    println("Solution 3 completed in " + (java.lang.System.currentTimeMillis() - startTime) + " ms")
    return df
  }

  def bootstrapping4(bootstrapsample: DataFrame) {
    val startTime = java.lang.System.currentTimeMillis()
    import org.apache.spark.sql.functions._
    val totalMap: Map[String, (Float, Float)] = Map()
    for (i <- 1 to N) {
      println("Step 5: resample " + i)
      // Step 5a
      val resample = bootstrapsample.sample(true, 1f);

      // Step 5b
      val df = resample.groupBy(KEY).agg(format_number(avg(VALUE), 2) as "Mean", format_number(variance(VALUE), 2) as "Variance")

      // Step 5c
      val map = dataframe2map(df)
      for ((k, v) <- map) {
        if (totalMap.contains(k)) {
          val value = totalMap(k)
          totalMap.put(k, (value._1 + v._1, value._2 + v._2))
        } else {
          totalMap.put(k, v)
        }
      }
    }
    println("Step 6:")
    for ((k, v) <- totalMap) {
      println(k + "\t" + (v._1 / N) + "\t" + (v._2 / N))
    }
    //    println(totalMap)
    println("Solution 4 completed in " + (java.lang.System.currentTimeMillis() - startTime) + " ms")
  }

  def dataframe2map(df: DataFrame): Map[String, (Float, Float)] = {
    val map: Map[String, (Float, Float)] = Map()
    val rows = df.collectAsList();
    //    println("\tInput Rows: " + rows)
    for (j <- 0 to rows.size() - 1) {
      val row = rows.get(j)
      map.put(row.getString(0), (row.getString(1).toFloat, row.getString(2).toFloat))
    }

    println("\tOutput Map: " + map)
    return map
  }

  def calc(orgDF: DataFrame, percentage: Float, actualMap: Map[String, (Float, Float)], meanWriter: PrintWriter, varianceWriter: PrintWriter) {
    print("\nCalculating percentage=" + percentage + ", N=" + N)
    val bootstrapsample = orgDF.sample(false, percentage)
    println(", " + bootstrapsample.count() + " samples")
    val df = bootstrapping1(bootstrapsample)
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

}