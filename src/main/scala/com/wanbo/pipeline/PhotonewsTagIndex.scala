package com.wanbo.pipeline

import com.alibaba.otter.canal.protocol.CanalEntry.{EventType, RowChange}
import com.wanbo.channel.ContentIndex
import com.wanbo.database.MysqlDriver
import com.wanbo.utils.Logging

import scala.collection.JavaConverters._

/**
  * Created by wanbo on 16/5/18.
  */
class PhotonewsTagIndex extends ContentIndex with Pipeline with Logging {

    private var mysql_driver: MysqlDriver = null

    init()

    def init(): Unit ={
        // Set content type 4 (Photonews)
        _contentType = 4
    }

    override def process(changedRow: RowChange): Unit = {

        log.info("****** Photonews tag index pipeline ******")

        val eventType = changedRow.getEventType

        val rowData = changedRow.getRowDatasList.asScala.toList

        rowData.foreach(data => {

            if(eventType == EventType.INSERT) {
                val afterCols = data.getAfterColumnsList.asScala
                val itemId = afterCols.find(_.getName == "photonewsid").head.getValue
                val itemCheadLine = afterCols.find(_.getName == "cn_title").head.getValue
                val itemTag = afterCols.find(_.getName == "tags").head.getValue
                val lead = afterCols.find(_.getName == "leadbody").head.getValue
                val pubDate = afterCols.find(_.getName == "add_times").head.getValue

                log.info("INSERT: ---->>> after: id[{}] - cheadline[{}] - tag [{}]", itemId, itemCheadLine, itemTag)

                if(itemId != "" && itemTag != ""){
                    val storyData = itemTag.split(",").filter(_!="").distinct.map(tag => (tag.trim, itemId, itemCheadLine, lead, pubDate))
                    if(storyData.nonEmpty) {
                        addProperties(_contentType, storyData)
                    }
                }

            } else if (eventType == EventType.DELETE) {
                val afterCols = data.getAfterColumnsList.asScala
                val itemId = afterCols.find(_.getName == "photonewsid").head.getValue
                val itemCheadLine = afterCols.find(_.getName == "cn_title").head.getValue
                val afterTag = afterCols.find(_.getName == "tags").head.getValue

                log.info("DELETE: ---->>> after: id[{}] - cheadline[{}] - tag [{}]", itemId, itemCheadLine, afterTag)

                if(itemId != "") {
                    delProperties(_contentType, itemId)
                }

            } else if (eventType == EventType.UPDATE) {
                val beforeCols = data.getBeforeColumnsList.asScala
                val beforeId = beforeCols.find(_.getName == "photonewsid").head.getValue
                val beforeCheadLine = beforeCols.find(_.getName == "cn_title").head.getValue
                val beforeTag = beforeCols.find(_.getName == "tags").head.getValue
                val beforeLead = beforeCols.find(_.getName == "leadbody").head.getValue

                val afterCols = data.getAfterColumnsList.asScala
                val afterId = afterCols.find(_.getName == "photonewsid").head.getValue
                val afterCheadLine = afterCols.find(_.getName == "cn_title").head.getValue
                val afterTag = afterCols.find(_.getName == "tags").head.getValue
                val afterLead = afterCols.find(_.getName == "leadbody").head.getValue
                val pubDate = afterCols.find(_.getName == "add_times").head.getValue

                log.info("UPDATE: ---->>> before: id[{}] - cheadline[{}] - tag [{}]", beforeId, beforeCheadLine, beforeTag)
                log.info("UPDATE: ---->>> after: id[{}] - cheadline[{}] - tag [{}]", afterId, afterCheadLine, afterTag)

                if (afterId != "") {

                    // Update tag
                    if (beforeTag != afterTag) {
                        log.info("The tag was changed, start to update tag.")

                        // Delete old data first
                        delProperties(_contentType, afterId)

                        // Then insert new data
                        val storyData = afterTag.split(",").filter(_ != "").distinct.map(tag => (tag.trim, afterId, afterCheadLine, afterLead, pubDate))
                        if (storyData.nonEmpty) {
                            addProperties(_contentType, storyData)
                        }

                    } else {
                        log.info("There is no necessary to update, the tag data didn't chang.")

                        // Update cheadline
                        if (beforeCheadLine != afterCheadLine) {
                            log.info("The Cheadline was changed, start to update Cheadline.")
                            updateCheadLine(afterId, _contentType, afterCheadLine)
                        }

                        // Update lead
                        if (beforeLead != afterLead) {
                            log.info("The lead was changed, start to update lead.")
                            updateLead(afterId, _contentType, afterLead)
                        }
                    }

                }

            } else {
                // Ignore
            }

        })
    }

    def addProperties(contentType: Int, dataList: Array[(String, String, String, String, String)]): Unit ={
        try {

            if (mysql_driver == null) {
                throw new Exception("Didn't initialize mysql driver.")
            }

            if(dataList.isEmpty){
                throw new Exception("There is no data to save to database.")
            }

            val conn = mysql_driver.getConnector("cmstmp01", writable = true)

            val sql = "INSERT INTO tag_content (`tag`, `contentid`, `type`, `cheadline`, `clead`, `pubdate`) VALUES " +
                dataList.map(x => "('%s','%s',%d,'%s','%s','%s')".format(x._1, x._2, contentType, x._3, x._4, x._5)).mkString(",")

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

    def delProperties(contentType: Int, contentId: String): Boolean ={
        var ret = false

        try {

            if (mysql_driver == null) {
                throw new Exception("Didn't initialize mysql driver.")
            }

            if(contentId == "") {
                throw new Exception("Must specify a photonews id.")
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
                throw new Exception("Must specify a photonews id.")
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

    def updateLead(contentId: String, contentType: Int, lead: String): Boolean ={
        var ret = false

        try {

            if (mysql_driver == null) {
                throw new Exception("Didn't initialize mysql driver.")
            }

            if(contentId == "") {
                throw new Exception("Must specify a photonews id.")
            }

            val conn = mysql_driver.getConnector("cmstmp01", writable = true)

            val sql = "UPDATE tag_content SET clead='%s' where contentid='%s' and `type`=%d".format(lead, contentId, contentType)

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

    override def setDriver(driver: MysqlDriver): Unit = {
        mysql_driver = driver
    }
}
