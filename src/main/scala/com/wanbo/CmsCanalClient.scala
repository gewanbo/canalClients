package com.wanbo

import java.lang.Thread.UncaughtExceptionHandler
import java.net.ConnectException
import java.text.SimpleDateFormat

import com.alibaba.otter.canal.client.CanalConnector
import com.alibaba.otter.canal.protocol.CanalEntry.{Entry, EntryType, EventType, RowChange}
import com.alibaba.otter.canal.protocol.Message
import com.alibaba.otter.canal.protocol.exception.CanalClientException
import com.wanbo.database.{Driver, MysqlDriver}
import com.wanbo.utils.Logging
import org.apache.commons.lang.SystemUtils
import org.springframework.util.Assert

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by wanbo on 16/5/11.
  */
class CmsCanalClient() extends Logging {
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
    var mysql_driver: MysqlDriver = null

    context_format = SEP + "****************************************************" + SEP
    context_format += "* Batch Id: [{}] ,count : [{}] , memsize : [{}] , Time : {}" + SEP
    context_format += "* Start : [{}] " + SEP
    context_format += "* End : [{}] " + SEP
    context_format += "****************************************************" + SEP

    def this(destination: String) {
        this()
        this.destination = destination
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

                    var contentType = 0 // 1: Story  2: video  3: interactive  4: photonews

                    if(schemaName == "cmstmp01" && tableName == "story") {

                        // Set content type
                        contentType = 1

                        log.info(SEP + "*** Schema Name: [{}] ,Table Name : [{}] , Delay time : [{}] ***" + SEP, schemaName, tableName, delayTime.toString)

                        val eventType = rowChange.getEventType

                        val rowData = rowChange.getRowDatasList.asScala.toList

                        rowData.foreach(data => {

                            if(eventType == EventType.INSERT) {
                                val afterCols = data.getAfterColumnsList.asScala
                                val itemId = afterCols.find(_.getName == "id").head.getValue
                                val itemCheadLine = afterCols.find(_.getName == "cheadline").head.getValue
                                val itemTag = afterCols.find(_.getName == "tag").head.getValue

                                log.info("INSERT: ---->>> after: id[{}] - cheadline[{}] - tag [{}]", itemId, itemCheadLine, itemTag)

                                if(itemId != "" && itemTag != ""){
                                    val storyData = itemTag.split(",").map(tag => (itemId, itemCheadLine, tag))
                                    if(storyData.nonEmpty) {
                                        addTags(contentType, storyData)
                                    }
                                }

                            } else if (eventType == EventType.DELETE) {
                                val afterCols = data.getAfterColumnsList.asScala
                                val itemId = afterCols.find(_.getName == "id").head.getValue
                                val itemCheadLine = afterCols.find(_.getName == "cheadline").head.getValue
                                val afterTag = afterCols.find(_.getName == "tag").head.getValue

                                log.info("DELETE: ---->>> after: id[{}] - cheadline[{}] - tag [{}]", itemId, itemCheadLine, afterTag)

                                if(itemId != "") {
                                    delTags(contentType, itemId)
                                }

                            } else if (eventType == EventType.UPDATE) {
                                val beforeCols = data.getBeforeColumnsList.asScala
                                val beforeId = beforeCols.find(_.getName == "id").head.getValue
                                val beforeCheadLine = beforeCols.find(_.getName == "cheadline").head.getValue
                                val beforeTag = beforeCols.find(_.getName == "tag").head.getValue

                                val afterCols = data.getAfterColumnsList.asScala
                                val afterId = afterCols.find(_.getName == "id").head.getValue
                                val afterCheadLine = afterCols.find(_.getName == "cheadline").head.getValue
                                val afterTag = afterCols.find(_.getName == "tag").head.getValue

                                log.info("UPDATE: ---->>> before: id[{}] - cheadline[{}] - tag [{}]", beforeId, beforeCheadLine, beforeTag)
                                log.info("UPDATE: ---->>> after: id[{}] - cheadline[{}] - tag [{}]", afterId, afterCheadLine, afterTag)

                                // Update tag
                                if(beforeTag != afterTag) {
                                    log.info("The tag was changed, start to update tag.")
                                    if (afterId != "" && afterTag != "") {
                                        // Delete old data first
                                        delTags(contentType, afterId)

                                        // Then insert new data
                                        val storyData = afterTag.split(",").map(tag => (afterId, afterCheadLine, tag))
                                        if (storyData.nonEmpty) {
                                            addTags(contentType, storyData)
                                        }
                                    }
                                } else {
                                    log.info("There is no necessary to update, the tag data didn't chang.")
                                }

                                // Update cheadline
                                if(beforeCheadLine != afterCheadLine){
                                    log.info("The Cheadline was changed, start to update Cheadline.")
                                    updateCheadLine(afterId, contentType, afterCheadLine)
                                }

                            } else {
                                // Ignore
                            }

                        })

                    }

                } catch {
                    case e: Exception =>
                        log.error("Parse event has a error, data:" + e.toString, e)
                }

            }
        })
    }

    def addTags(contentType: Int, dataList: Array[(String, String, String)]): Unit ={
        try {

            if (mysql_driver == null) {
                throw new Exception("Didn't initialize mysql driver.")
            }

            if(dataList.isEmpty){
                throw new Exception("There is no data to save to database.")
            }

            val conn = mysql_driver.getConnector("cmstmp01", writable = true)

            val sql = "INSERT INTO tag_content VALUES " + dataList.map(x => "('%s','%s',%d,'%s')".format(x._3, x._1, contentType, x._2)).mkString(",")

            log.info("SQL:" + sql)

            val ps = conn.prepareStatement(sql)
            val rs = ps.executeUpdate()

            log.info("--->>INSERT Successful>>>--::::" + rs)

            ps.close()
            conn.close()
        } catch {
            case e: Exception =>
                log.error("There something error:", e)
        }
    }

    def delTags(contentType: Int, contentId: String): Boolean ={
        var ret = false

        try {

            if (mysql_driver == null) {
                throw new Exception("Didn't initialize mysql driver.")
            }

            if(contentId == "") {
                throw new Exception("Must specify a story id.")
            }

            val conn = mysql_driver.getConnector("cmstmp01", writable = true)

            val sql = "DELETE FROM tag_content where contentid='%s' and `type`=%d".format(contentId, contentType)

            val ps = conn.prepareStatement(sql)
            val rs = ps.executeUpdate()

            log.info("--->>DELETE Successful>>>--::::" + rs)

            if(rs > 0)
                ret = true

            ps.close()
            conn.close()
        } catch {
            case e: Exception =>
                log.error("There something error:", e)
        }

        ret
    }

    def updateCheadLine(contentId: String, contentType: Int, cheadLine: String): Boolean ={
        var ret = false

        try {

            if (mysql_driver == null) {
                throw new Exception("Didn't initialize mysql driver.")
            }

            if(contentId == "") {
                throw new Exception("Must specify a story id.")
            }

            val conn = mysql_driver.getConnector("cmstmp01", writable = true)

            val sql = "UPDATE tag_content SET cheadline='%s' where contentid='%s' and `type`=%d".format(cheadLine, contentId, contentType)

            val ps = conn.prepareStatement(sql)
            val rs = ps.executeUpdate()

            log.info("--->>UPDATE Successful>>>--::::" + rs)

            if(rs > 0)
                ret = true

            ps.close()
            conn.close()
        } catch {
            case e: Exception =>
                log.error("There something error:", e)
        }

        ret
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

    def setDBDriver(driver: Driver): Unit = {
        driver match {
            case mysql: MysqlDriver =>
                this.mysql_driver = mysql
            case _ =>
        }
    }
}
