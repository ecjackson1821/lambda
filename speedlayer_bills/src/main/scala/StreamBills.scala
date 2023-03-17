import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Increment, Put}
import org.apache.hadoop.hbase.util.Bytes

object StreamBills {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", "localhost")

  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val table = hbaseConnection.getTable(TableName.valueOf("chi_hosp"))
//  val inputtedBills = hbaseConnection.getTable(TableName.valueOf(name = ""))

  def incrementInvoicesByProcedureHospital(kfr : HospitalBillsReport) : String = {

    val inc = new Increment(Bytes.toBytes(kfr.code_hospital))
    inc.addColumn(Bytes.toBytes("charges"), Bytes.toBytes("num_inputted_bills"), 1)
    inc.addColumn(Bytes.toBytes("charges"), Bytes.toBytes("inputted_bills_sum"), kfr.billed_amount)
    table.increment(inc)
    return "Updated speed layer for hospital procedure" + kfr.code_hospital
  }

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
                            |Usage: StreamBills <brokers>
                            |  <brokers> is a list of one or more Kafka brokers
                            |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamBills")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("ecjackson_hospital_input")
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    // Get the lines, split them into words, count the words and print
    val serializedRecords = stream.map(_.value);
//    val reports = serializedRecords.map(rec => mapper.readValue(rec, classOf[HospitalBillsReport]))
    val kfrs = serializedRecords.map(rec => mapper.readValue(rec, classOf[HospitalBillsReport]))

    //update speed table
    val processedInvoices = kfrs.map(incrementInvoicesByProcedureHospital)
    processedInvoices.print()
    // How to write to an HBase table
//    val batchStats = reports.map(bill => {
//      print(bill)
//      val put = new Put(Bytes.toBytes(bill.code_hospital))
//      put.addColumn(Bytes.toBytes("bills"), Bytes.toBytes("category"), Bytes.toBytes(bill.category))
//      put.addColumn(Bytes.toBytes("bills"), Bytes.toBytes("procedure_code"), Bytes.toBytes(bill.procedure_code))
//      put.addColumn(Bytes.toBytes("bills"), Bytes.toBytes("hospital"), Bytes.toBytes(bill.hospital))
//      put.addColumn(Bytes.toBytes("bills"), Bytes.toBytes("billed_amount"), Bytes.toBytes(bill.billed_amount))
//      table.put(put)
//    })
//    batchStats.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}

