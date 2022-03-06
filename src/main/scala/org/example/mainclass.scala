package org.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import io.delta.tables._
import org.apache.spark.sql.functions._


object mainclass  {

  def main(args: Array[String]): Unit = {
    lazy val spark = getSparkSession()

    val ratingPath = "data/ml-latest-small/ratings/*"
    val moviePath = "data/ml-latest-small/movies/*"
    val tagsPath = "data/ml-latest-small/tags/*"


    val ratings = readcsv(ratingPath)(spark)
    val movies = readcsv(moviePath)(spark)
    val tags = readcsv(tagsPath)(spark)

    ratings.printSchema()
    movies.printSchema()
    tags.printSchema()



    val columnsDataTypeMap:Map[String,String] = Map(("userId","Int"),("movieId","Int"),("rating","Int"))

    val ratingsCasted = castDataTypes(ratings,columnsDataTypeMap)
    val moviesCasted = castDataTypes(movies,columnsDataTypeMap)
    val tagsCasted = castDataTypes(tags,columnsDataTypeMap)


    val ratingsCastedAddedColumn = ratingsCasted
      .withColumn("Datepart",date_format(col("current_timestamp"),"yyyy-MM-dd"))


    ratingsCastedAddedColumn.printSchema()
    moviesCasted.printSchema()
    tagsCasted.printSchema()


    if (!DeltaTable.isDeltaTable(spark, "deltatables/ratings") ) {
      println("table doesnt exist")
      insertIntoDelta(ratingsCastedAddedColumn,"overwrite","delta","deltatables/ratings","Datepart")
    }
   else {
       println("table exist")
      val deltaTable = DeltaTable.forPath("deltatables/ratings")
      deltaTable.as("oldData")
        .merge(
          ratingsCastedAddedColumn.as("newData"),
          (
            "oldData.userId = newData.userId and oldData.movieId = newData.movieID")
        )
        .whenMatched
        .updateAll()
        .whenNotMatched
        .insertAll()
        .execute()
    }
    insertIntoDelta(moviesCasted,"overwrite","delta","deltatables/movies",null)
    insertIntoDelta(tagsCasted,"overwrite","delta","deltatables/tags",null)




    val moviesEditedGenres = moviesCasted.withColumn("Genres",regexp_replace(col("genres"),"\\|"," "))
    moviesEditedGenres.show()

    val df3 = ratings.selectExpr("movieId")
      .groupBy("movieID")
      .count()
      .where(col("count") >= 5).orderBy(desc("count")).limit(10)

    val topTenMovies = df3.join(broadcast(moviesCasted),df3("movieId") === moviesCasted("movieId"))
      .drop(moviesCasted("movieId"))
      .selectExpr("movieId","title","count")

    topTenMovies.coalesce(1).write.mode("overwrite").csv("data/ml-latest-small/output/")


  }

  def getSparkSession():SparkSession = {
    val spark = SparkSession.builder().master("local[1]")
      .getOrCreate();
    spark.sparkContext.setLogLevel("Error")
    spark

  }

  def readcsv(path:String)(implicit spark: SparkSession):DataFrame = {
    val df = spark.read.format("csv").option("header","true").load(path)
    df
  }

  def insertIntoDelta(df:DataFrame,mode:String,format:String,deltaPath:String,partitionColumns:String): Unit = {
   if(partitionColumns != null)
     df.write.mode(mode).option("overwriteSchema", "true").partitionBy(partitionColumns).format(format).save(deltaPath)
   else
     df.write.mode(mode).option("overwriteSchema", "true").format(format).save(deltaPath)
  }

  def castDataTypes(df:DataFrame,columnsDataTypeMap:Map[String,String]):DataFrame = {
    val df1 =  df.columns.foldLeft(df){
      (df,colname) => {
        if (columnsDataTypeMap.keys.toList.contains(colname)) {
          df.withColumn(colname, col(colname).cast(columnsDataTypeMap.get(colname).get))
        }
        else {
          df
        }
      }
    }
    df1
  }



}


