package fpm

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.ml.fpm.FPGrowth


object FPGrowthModel extends App{

  val sc = new SparkContext("local[*]","Market Basket Analysis using FP-Growth Spark - FP-Growth Model")
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._


  //  Load Data sets
  val aisles = sqlContext.read.format("com.databricks.spark.csv").
    option("header", "true")
    .option("inferSchema", "true")
    .load("D:\\Spark\\instacart_2017_05_01\\aisles.csv")

  val departments = sqlContext.read.format("com.databricks.spark.csv").
    option("header", "true")
    .option("inferSchema", "true")
    .load("D:\\Spark\\instacart_2017_05_01\\departments.csv")

  val order_products_prior = sqlContext.read.format("com.databricks.spark.csv").
    option("header", "true")
    .option("inferSchema", "true")
    .load("D:\\Spark\\instacart_2017_05_01\\order_products__prior.csv")

  val order_products_train = sqlContext.read.format("com.databricks.spark.csv").
    option("header", "true")
    .option("inferSchema", "true")
    .load("D:\\Spark\\instacart_2017_05_01\\order_products__train.csv")

  val orders = sqlContext.read.format("com.databricks.spark.csv").
    option("header", "true")
    .option("inferSchema", "true")
    .load("D:\\Spark\\instacart_2017_05_01\\orders.csv")

  val products = sqlContext.read.format("com.databricks.spark.csv").
    option("header", "true")
    .option("inferSchema", "true")
    .load("D:\\Spark\\instacart_2017_05_01\\products.csv")


  //  Print Each DataFrames Schema
  aisles.printSchema()
  departments.printSchema()
  order_products_prior.printSchema()
  order_products_train.printSchema()
  orders.printSchema()
  products.printSchema()


  //  Create temporary tables forEach data frame
  aisles.createOrReplaceTempView("aisles")
  departments.createOrReplaceTempView("departments")
  order_products_prior.createOrReplaceTempView("order_products_prior")
  order_products_train.createOrReplaceTempView("order_products_train")
  orders.createOrReplaceTempView("orders")
  products.createOrReplaceTempView("products")


//  Organize the data by shopping basket
  val rawData = sqlContext.sql("SELECT " +
                                              "p.product_name, " +
                                              "o.order_id " +
                                        "FROM products p " +
                                        "INNER JOIN order_products_train o " +
                                        "ON o.product_id = p.product_id")


  val baskets = rawData.groupBy("order_id").agg(collect_set("product_name").alias("items"))
  baskets.createOrReplaceTempView("baskets")

  baskets.printSchema()
  baskets.show(20)

//  Train Model

  val basket_df = sqlContext.sql("SELECT " +
                                            "items " +
                                            "FROM baskets").as[Array[String]].toDF("items")

//  FP-Growth
  val fpGrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.001).setMinConfidence(0).setNumPartitions(10)
  val fpGrowthModel = fpGrowth.fit(basket_df)

//  1. Most Frequent Items in the Basket
  val mostFrequentItems = fpGrowthModel.freqItemsets
  mostFrequentItems.createOrReplaceTempView("mostFrequentItems")

  val displayMostFrequentItems = sqlContext.sql("SELECT " +
                                                              "items, " +
                                                              "freq " +
                                                        "FROM mostFrequentItems " +
                                                        "WHERE size(items) > 2 " +
                                                        "ORDER BY freq DESC")

  displayMostFrequentItems.show(20)

//  2. Association Rules
  val associationFrequency = fpGrowthModel.associationRules
  associationFrequency.createOrReplaceTempView("associationFrequency")

  val displayAssociationFrequency = sqlContext.sql("SELECT " +
                                                                "antecedent AS `antecedent (if)`, " +
                                                                "consequent AS `consequent (then)`, " +
                                                                "confidence " +
                                                            "FROM associationFrequency " +
                                                            "ORDER BY confidence DESC")


  displayAssociationFrequency.show(20)

}
