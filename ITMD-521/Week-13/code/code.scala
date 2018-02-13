
import com.databricks.spark.avro._
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val category= sqlContext.read.avro("hdfs://localhost/user/vagrant/spark/input/categories.avro")
category.show()
import com.databricks.spark.avro._
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val product = sqlContext.read.avro("hdfs://localhost/user/vagrant/spark/input/products.avro")
product.show()

import com.databricks.spark.avro._
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val prod= sqlContext.read.avro("hdfs://localhost/user/vagrant/spark/input/products.avro")
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
val df1 = prod.selectExpr("_1","_2","_3","cast(_4 as float) _4","_5")
val rdd_rows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = df1.rdd
val newRDD = rdd_rows.filter(t => (t(3) != null))
val sourceRdd = newRDD.map(_.mkString(","))
val m = sourceRdd.map(_.split(",")).filter(_(3).toFloat < 100)
val ms = m.map(_.mkString(","))
ms.coalesce(1).saveAsTextFile("hdfs://localhost/user/vagrant/output/1/")


import com.databricks.spark.avro._
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val prod= sqlContext.read.avro("hdfs://localhost/user/vagrant/spark/input/products.avro")
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
val df1 = prod.selectExpr("_1","_2","_3","cast(_4 as float) _4","_5")
val rdd_rows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = df1.rdd
val newRDD = rdd_rows.filter(t => (t(3) != null))
val sourceRdd = newRDD.map(_.mkString(","))
val m = sourceRdd.map(_.split(",")).filter(_(3).toFloat < 100)
val ms = m.map(_.mkString(","))
import com.databricks.spark.avro._
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val category= sqlContext.read.avro("hdfs://localhost/user/vagrant/spark/input/categories.avro")
case class product(product_id: String, category_id: String, product_name: String, price: String, link: String)
val txt = ms.map(line => line.split(",").map(_.trim))
val pro = txt.map{case Array(s0, s1, s2, s3,s4) => product(s0, s1, s2, s3,s4)}.toDF
val cat_seq = Seq("category_id", "category_name")
val catdf = category.toDF(cat_seq: _*)
val cat = catdf.selectExpr("category_id","category_name")
val join_df = cat.join(pro, "category_id")
import org.apache.spark.sql.expressions.Window
val baseDF =  join_df.select(col("category_id"), col("category_name"), col("product_name"), col("price") ,row_number().over(Window.partitionBy(col("category_id")).orderBy(col("price").desc)).alias("Row_Num"))
val filDF = baseDF.where(baseDF("Row_Num") <=10)
val selDF = filDF.select(col("category_name"),col("product_name"),col("price"))
val sortDF = selDF.sort(col("category_name"),col("product_name"),col("price"))
val rdd_Final: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = sortDF.rdd
val rdd_dl = rdd_Final.map(_.mkString("\t"))
rdd_dl.coalesce(1).saveAsTextFile("hdfs://localhost/user/vagrant/output/2/")


import com.databricks.spark.avro._
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val prod= sqlContext.read.avro("hdfs://localhost/user/vagrant/spark/input/products.avro")
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
val df1 = prod.selectExpr("_1","_2","_3","cast(_4 as float) _4","_5")
val rdd_rows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = df1.rdd
val newRDD = rdd_rows.filter(t => (t(3) != null))
val sourceRdd = newRDD.map(_.mkString(","))
val m = sourceRdd.map(_.split(","))
val ms = m.map(_.mkString(","))
import com.databricks.spark.avro._
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val category= sqlContext.read.avro("hdfs://localhost/user/vagrant/spark/input/categories.avro")
case class product(product_id: String, category_id: String, product_name: String, price: String, link: String)
val txt = ms.map(line => line.split(",").map(_.trim))
val pro = txt.map{case Array(s0, s1, s2, s3,s4) => product(s0, s1, s2, s3,s4)}.toDF
val cat_seq = Seq("category_id", "category_name")
val catdf = category.toDF(cat_seq: _*)
val cat = catdf.selectExpr("category_id","category_name")
val join_df = cat.join(pro, "category_id")
import org.apache.spark.sql.expressions.Window
val max_seq = Seq("category_id", "category_name", "highest_product_name", "highest_product_price","Row_Num_high")
val min_seq = Seq("category_id", "category_name_low", "lowest_product_name", "lowest_product_price","Row_Num_low")
val max_DF =  join_df.select(col("category_id"), col("category_name"), col("product_name"), col("price") ,row_number().over(Window.partitionBy(col("category_id")).orderBy(col("price").desc)).alias("Row_Num"))
val maxDF = max_DF.where(max_DF("Row_Num") <= 1)
val min_DF =  join_df.select(col("category_id"), col("category_name"), col("product_name"), col("price") ,row_number().over(Window.partitionBy(col("category_id")).orderBy(col("price").asc)).alias("Row_Num"))
val minDF = min_DF.where(min_DF("Row_Num") <= 1)
val highDF = maxDF.toDF(max_seq: _*)
val lowDF = minDF.toDF(min_seq: _*)
val prosDF = highDF.join(lowDF,"category_id")
val finalDF = prosDF.select(col("Category_name"),col("highest_product_name"),col("highest_product_price"),col("lowest_product_name"),col("lowest_product_price"))
val rdd_Final: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = finalDF.rdd
val rdd_dl = rdd_Final.map(_.mkString("|"))

finalDF.coalesce(1).write.option("delimiter","|").avro("hdfs://localhost/user/vagrant/output/4/")