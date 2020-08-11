package fpm

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Hello world!
 *
 */
object DataExploration extends App {

  val sc = new SparkContext("local[*]","Market Basket Analysis using FP-Growth Spark - Data Exploration")
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


//  Data Analysis
//   1. Orders By Day of Week
  val orders_by_day_of_week = sqlContext.sql("SELECT COUNT(order_id) AS total_orders," +
    "(CASE " +
        "WHEN order_dow = '0' THEN 'Sunday'" +
        "WHEN order_dow = '1' THEN 'Monday'  " +
        "WHEN order_dow = '2' THEN 'Tuesday' " +
        "WHEN order_dow = '3' THEN 'Wednesday' " +
        "WHEN order_dow = '4' THEN 'Thursday' " +
        "WHEN order_dow = '5' THEN 'Friday'  " +
        "WHEN order_dow = '6' THEN 'Saturday' " +
    "END) AS day_of_week FROM orders " +
    "GROUP BY day_of_week ORDER BY total_orders DESC")

  orders_by_day_of_week.show()


//  2.Orders By Each Hour
  val orders_by_hours = sqlContext.sql("SELECT COUNT(order_id) AS total_orders," +
                                                      "order_hour_of_day as hour " +
                                                "FROM orders " +
                                                "GROUP BY order_hour_of_day " +
                                                "ORDER BY order_hour_of_day")
  orders_by_hours.show()


//  3.Sum ordered count of each product
  val sum_order_count_for_each_product = sqlContext.sql("SELECT " +
                                                                    "products.product_id," +
                                                                    "products.product_name, " +
                                                                    "SUM(order_products_prior.add_to_cart_order) AS order_count " +
                                                                "FROM order_products_prior " +
                                                                "INNER JOIN products " +
                                                                "ON products.product_id = order_products_prior.product_id "+
                                                                "GROUP BY products.product_id,products.product_name " +
                                                                "ORDER BY order_count DESC")
  sum_order_count_for_each_product.show(20)


  // 4.Sum orders count of each user
  val sum_order_count_for_each_user = sqlContext.sql("SELECT " +
                                                                "orders.user_id , " +
                                                                "SUM(order_products_prior.add_to_cart_order) AS order_count " +
                                                            "FROM order_products_prior " +
                                                            "INNER JOIN orders " +
                                                            "ON orders.order_id = order_products_prior.order_id "+
                                                            "GROUP BY orders.user_id "+
                                                            "ORDER BY order_count DESC")
  sum_order_count_for_each_user.show(20)


//  5.Top 20 products
  val top_20_products = sqlContext.sql("SELECT " +
                                                    "COUNT(opp.order_id) AS orders, " +
                                                    "p.product_name AS popular_product  " +
                                                "FROM order_products_prior opp " +
                                                "INNER JOIN products p " +
                                                "ON p.product_id = opp.product_id " +
                                                "GROUP BY popular_product " +
                                                "ORDER BY orders DESC " +
                                                "LIMIT 20")
  top_20_products.show()


//  6. Number of products for each aisle
  val product_by_aisle = sqlContext.sql("SELECT " +
                                                    "COUNT(p.product_id) as product_count, " +
                                                    "a.aisle, " +
                                                    "a.aisle_id " +
                                                "FROM aisles a " +
                                                "INNER JOIN products p " +
                                                "ON a.aisle_id = p.aisle_id " +
                                                "GROUP BY a.aisle_id,a.aisle " +
                                                "ORDER BY product_count DESC")
  product_by_aisle.show(20)


//  7. Shelf space by each department
  val shel_space_in_each_department = sqlContext.sql("SELECT " +
                                                                  "d.department," +
                                                                  "count(distinct p.product_id) AS product_count  " +
                                                              "FROM products p " +
                                                              "INNER JOIN departments d " +
                                                              "ON d.department_id = p.department_id " +
                                                              "GROUP BY d.department " +
                                                              "ORDER BY product_count DESC")
  shel_space_in_each_department.show()


// 8. Products by Departments
  val products_by_departments = sqlContext.sql("SELECT countbydept.* " +
                                                       "FROM ( " +
                                                            "SELECT " +
                                                                "department_id, " +
                                                                "COUNT(1) AS counter " +
                                                            "FROM products" +
                                                            "GROUP BY department_id" +
                                                            "ORDER BY counter ASC  ) as maxcount "+
                                                        "INNER JOIN ( " +
                                                              "SELECT " +
                                                              "d.department_id, " +
                                                              "d.department, " +
                                                              "COUNT(1) AS products " +
                                                        "FROM department d ) " +
                                                              "INNER JOIN products p " +
                                                              "ON p.department_id = d.department_id " +
                                                        "GROUP by d.department_id, d.department " +
                                                        "ORDER by products DESC " +
                                                        ") countbydept " +
                                                        "ON countbydept.products = maxcount.counter")

  products_by_departments.show()

}
