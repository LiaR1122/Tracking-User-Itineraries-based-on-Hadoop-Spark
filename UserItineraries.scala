// Copyright: Group 29
//        ID: 1928620 1928512 1927932

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, ConnectionFactory, Put, TableDescriptorBuilder}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// This Spark Implementation
object UserItineraries {

  // Create Table in HBase
  def createSchemaTables(config: Configuration, table_name1: String): Unit = {
      val connection = ConnectionFactory.createConnection(config)
      val admin = connection.getAdmin
      try {
        // Table Initializing
        val table1 = TableName.valueOf(table_name1)
        val tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(table1)
        val columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("User"))
        val cfDes = columnFamilyDescriptorBuilder.build
        tableDescriptorBuilder.setColumnFamily(cfDes)
        val tableDescriptor = tableDescriptorBuilder.build
        System.out.print("Creating table 1. ")

        // If exists, disable and delete it
        if (admin.tableExists(table1)) {
          admin.disableTable(table1)
          admin.deleteTable(table1)
        }
        // Create Table
        admin.createTable(tableDescriptor)
        System.out.println(" Done.")
      } finally {
        if (connection != null) connection.close()
        if (admin != null) admin.close()
      }
  }

  // Parsing logs
  def parseLogs(sc: SparkContext, path: String): RDD[(String, String)] = {
    val lines: RDD[String] = sc.textFile(path)
    val sp1 = lines.map(line => {
      val data = line.split(",")
      val lac = data(1)
      val cell = data(2)
      val mobile = data(3)
      val id = lac + cell
      (id, mobile)
    })
    sp1
  }

  // Parsing CSV
  def parseCSV(sc: SparkContext, path: String): RDD[(String, String)] = {
    val address: RDD[String] = sc.textFile(path)
    val header = address.first()
    val Cstat = address.filter(_ != header)
    val sp2 = Cstat.map(line => {
      val data = line.split(",")
      val lac = data(2)
      val cell = data(3)
      val city = data(12)
      val id = lac + cell
      (id, city)
    })
    sp2
  }


  // Main Function
  def main(args: Array[String]): Unit = {
    case class Log(ID: String, Num: String)
    case class Cell(ID: String, City: String)

    if (args.length != 2) {
      System.err.println("You need to specify the input(csv and logs) path in HDFS" + "{ args[0]: csv_path, args[1]: logs_path }")
      System.exit(2)
    }

    // Spark Config
    val conf = new SparkConf()
      .setAppName("UseItinerariesSpark")
    //      .setMaster("local")                           // Set local while debugging
    val sc = new SparkContext(conf)

    // Retreving the itineraries of each phone number
    val sp1 = parseCSV(sc, args(0))
    val sp2 = parseLogs(sc, args(1))

    val join: RDD[(String, String)] = sp2.join(sp1).map(rdd => {
      val num = rdd._2._1
      val city = rdd._2._2
      (num, city)
    })
    val result = join.distinct().map(x => (x._1, x._2.substring(1, x._2.length - 1))).reduceByKey(_ + "," + _)


    // HBase Config
    val hbaseConf = HBaseConfiguration.create()
    val tablename = "UserItinerariesSpark"
    hbaseConf.set("hbase.zookeeper.quorum", "localhost")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

    // Create Table
    createSchemaTables(hbaseConf, tablename)

    // Insert into HBase
    val jobConf = new JobConf()
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)
    val rdd: Unit = result.map(line => {
      val key = line._1
      val city = line._2
      val put = new Put(Bytes.toBytes(key))
      put.addColumn(Bytes.toBytes("User"), Bytes.toBytes("Itineraries"), Bytes.toBytes(city))
      (new ImmutableBytesWritable, put)
    }).saveAsHadoopDataset(jobConf)

    sc.stop()
  }
}
