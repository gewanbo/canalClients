package com.wanbo

import java.io.FileInputStream
import java.lang.Thread.UncaughtExceptionHandler
import java.net.{ConnectException, InetSocketAddress}
import java.text.SimpleDateFormat
import java.util.Properties

import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.CanalEntry.{Entry, EntryType, RowChange}
import com.alibaba.otter.canal.protocol.Message
import com.alibaba.otter.canal.protocol.exception.CanalClientException
import com.wanbo.database.{DriverPool, HBaseDriver}
import com.wanbo.pipeline.{Pipeline, Story2HBase}
import com.wanbo.utils.Logging
import org.apache.commons.lang.SystemUtils
import org.springframework.util.Assert

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by wanbo on 16/6/20.
  */
object Cms2HBaseCanalClient extends Logging {
    def main(args: Array[String]) {
        try {
            val config = new Properties()

            log.info("Start to load config file.")
            val configFile = System.getProperty("easy.conf")
            log.info("Config file:" + configFile)

            val is = new FileInputStream(configFile)
            config.loadFromXML(is)
            is.close()

            log.info("Config file load successful!")

            // Initialize data source
            val dbConf = new Properties()
            dbConf.put("hbase.host", config.getProperty("hbase.host"))

            val hbaseDriver = new HBaseDriver
            hbaseDriver.setConfiguration(dbConf)

            DriverPool.appendDriver(hbaseDriver)

            // Initialize pipeline list
            var pipelines = Map[String, Pipeline]()

            val storySubscribe = "cmstmp01.story"
            val story2HBase: Pipeline = new Story2HBase
            pipelines = pipelines + (storySubscribe -> story2HBase)

            // Initialize canal
            val canalServer = config.getProperty("canal.server")
            val canalPort = config.getProperty("canal.port").toInt
            val destination = config.getProperty("canal.instance")

            log.info("canal: server-{} port-{} instance-{}", canalServer, canalPort.toString, destination)

            val connector = CanalConnectors.newSingleConnector(new InetSocketAddress(canalServer, canalPort), destination, "", "")

            val cms2HBaseClient = new Cms2HBaseCanalClient(destination, pipelines)
            cms2HBaseClient.setConnector(connector)
            cms2HBaseClient.start()

            Runtime.getRuntime.addShutdownHook(new Thread{
                override def run(): Unit ={
                    try {
                        cms2HBaseClient.stop()
                    } catch {
                        case e: Exception =>
                            log.warn("Something goes wrong when stopping canal:", e)
                    } finally {
                        log.info("### Cannal client is down. ###")
                    }
                }
            })

        } catch {
            case e: Exception =>
                log.error("Error:", e)
        }
    }
}


class Cms2HBaseCanalClient() extends Logging {
    val SEP: String                    = SystemUtils.LINE_SEPARATOR
    val DATE_FORMAT: String            = "yyyy-MM-dd HH:mm:ss"
    var running = false
    val handler                        = new UncaughtExceptionHandler {
        override def uncaughtException(t: Thread, e: Throwable): Unit = {
            log.error("parse events has an error", e)
        }
    }
    var thread: Thread = null
    var connector: CanalConnector = null
    var context_format: String         = ""
    val row_format: String             = ""
    val transaction_format: String     = ""
    var destination: String = ""

    var pipelines: Map[String, Pipeline] = Map[String, Pipeline]()

    context_format = SEP + "****************************************************" + SEP
    context_format += "* Batch Id: [{}] ,count : [{}] , memsize : [{}] , Time : {}" + SEP
    context_format += "* Start : [{}] " + SEP
    context_format += "* End : [{}] " + SEP
    context_format += "****************************************************" + SEP

    def this(destination: String) {
        this()
        this.destination = destination
    }

    def this(destination: String, pipelines: Map[String, Pipeline]) {
        this()
        this.destination = destination
        this.pipelines = pipelines
    }

    def process(): Unit ={
        val batchSize = 5 * 1024
        while (running) {
            try {
                connector.connect()
                connector.subscribe("cmstmp01\\..*")
                while (running) {
                    val message = connector.getWithoutAck(batchSize)
                    val batchId = message.getId
                    val size = message.getEntries.size()
                    if(batchId == -1 || size == 0) {
                        // Sleep()
                    } else {
                        // Do something
                        printSummary(message, batchId, size)
                    }

                    connector.ack(batchId)
                    //connector.rollback(batchId)
                }
            } catch {
                case cce: CanalClientException =>
                    log.error("Canal client exception:", cce)

                    if(cce.getCause.isInstanceOf[ConnectException]){
                        Thread.sleep(3000)
                    }

                case e: Exception =>
                    log.error("Process error:", e)
            } finally {
                connector.disconnect()
            }
        }
    }

    def printSummary(message: Message, batchId: Long, size: Int): Unit ={
        var memSize = 0L
        val entries = message.getEntries.asScala
        entries.foreach(e => {
            memSize += e.getHeader.getEventLength
        })

        var startPosition = ""
        var endPosition = ""
        if(entries.nonEmpty){
            startPosition = buildPositionForDump(entries.head)
            endPosition = buildPositionForDump(entries.last)
        }

        val format = new SimpleDateFormat(DATE_FORMAT)
        log.info(context_format, batchId.toString, size.toString, memSize.toString, format.format(System.currentTimeMillis()), startPosition, endPosition)

        // Do some work
        processEntries(entries)
    }

    def buildPositionForDump(entry: Entry): String ={
        val header = entry.getHeader
        val time = header.getExecuteTime
        val format = new SimpleDateFormat(DATE_FORMAT)
        "%s:%d:%d(%s)".format(header.getLogfileName, header.getLogfileOffset, time, format.format(time))
    }

    def processEntries(entries: mutable.Buffer[Entry]): Unit ={

        entries.foreach(entry => {
            val header = entry.getHeader

            val executeTime = header.getExecuteTime
            val delayTime  = System.currentTimeMillis() - executeTime

            val schemaName = header.getSchemaName
            val tableName = header.getTableName

            if(entry.getEntryType == EntryType.ROWDATA) {

                try {

                    val rowChange = RowChange.parseFrom(entry.getStoreValue)
                    log.info(SEP + "*** Schema Name: [{}] ,Table Name : [{}] , Delay time : [{}] ***" + SEP, schemaName, tableName, delayTime.toString)

                    pipelines.foreach({ case (target: String, pipeline: Pipeline) =>
                            if(schemaName + "." + tableName == target)
                                pipeline.process(rowChange)
                    })

                } catch {
                    case e: Exception =>
                        log.error("Parse event has a error, data:" + e.toString, e)
                }

            }
        })
    }

    def start(): Unit = {
        Assert.notNull(connector, "connector is null")
        thread = new Thread(new Runnable {
            override def run(): Unit = {
                process()
            }
        })

        thread.setUncaughtExceptionHandler(handler)
        thread.start()
        running = true
    }

    def stop(): Unit ={
        if(running){
            if(thread != null){
                try {
                    log.info("---------------- running ----------------")
                    thread.join(3000)
                } catch {
                    case e: Exception =>
                        log.error("Error:", e)
                }
            }
        } else {
            running = false
        }
    }

    def setConnector(connector: CanalConnector): Unit ={
        this.connector = connector
    }

}
