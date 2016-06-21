package com.wanbo.pipeline

import com.alibaba.otter.canal.protocol.CanalEntry.{EventType, RowChange}
import com.wanbo.channel.ContentIndex
import com.wanbo.database.{DriverPool, MysqlDriver}
import com.wanbo.utils.Logging

import scala.collection.JavaConverters._

/**
  * Created by wanbo on 16/5/17.
  */
class StoryTagIndex extends ContentIndex with Pipeline with Logging {

    private val mysql_driver: MysqlDriver = DriverPool.getDriver("mysql", isWritable = true).orNull.asInstanceOf[MysqlDriver]

    private val keepTotalNum = 5

    init()

    def init(): Unit ={
        // Set content type 1 (Story)
        _contentType = 1
    }

    override def process(changedRow: RowChange): Unit = {

        log.info("****** Story tag index pipeline ******")

        val eventType = changedRow.getEventType

        val rowData = changedRow.getRowDatasList.asScala.toList

        rowData.foreach(data => {

            if(eventType == EventType.INSERT) {
                val afterCols = data.getAfterColumnsList.asScala
                val itemId = afterCols.find(_.getName == "id").head.getValue
                val itemCheadLine = afterCols.find(_.getName == "cheadline").head.getValue
                val itemTag = afterCols.find(_.getName == "tag").head.getValue
                val cShortLead = afterCols.find(_.getName == "cshortleadbody").head.getValue
                val cLongLead = afterCols.find(_.getName == "clongleadbody").head.getValue
                val pubDate = afterCols.find(_.getName == "last_publish_time").head.getValue

                val cLead = if(cLongLead.length > cShortLead.length) cLongLead else cShortLead

                log.info("INSERT: ---->>> after: id[{}] - cheadline[{}] - tag [{}]", itemId, itemCheadLine, itemTag)

                if(itemId != "" && itemTag != ""){
                    val storyData = itemTag.split(",").filter(_!="").distinct.map(tag => (tag.trim, itemId, itemCheadLine, cLead, pubDate))
                    if(storyData.nonEmpty) {
                        addProperties(_contentType, storyData)
                    }
                }

            } else if (eventType == EventType.DELETE) {
                val afterCols = data.getAfterColumnsList.asScala
                val itemId = afterCols.find(_.getName == "id").head.getValue
                val itemCheadLine = afterCols.find(_.getName == "cheadline").head.getValue
                val afterTag = afterCols.find(_.getName == "tag").head.getValue

                log.info("DELETE: ---->>> after: id[{}] - cheadline[{}] - tag [{}]", itemId, itemCheadLine, afterTag)

                if(itemId != "") {
                    delProperties(_contentType, itemId)
                }

            } else if (eventType == EventType.UPDATE) {
                val beforeCols = data.getBeforeColumnsList.asScala
                val beforeId = beforeCols.find(_.getName == "id").head.getValue
                val beforeCheadLine = beforeCols.find(_.getName == "cheadline").head.getValue
                val beforeTag = beforeCols.find(_.getName == "tag").head.getValue
                val beforeShortLead = beforeCols.find(_.getName == "cshortleadbody").head.getValue
                val beforeLongLead = beforeCols.find(_.getName == "clongleadbody").head.getValue

                val afterCols = data.getAfterColumnsList.asScala
                val afterId = afterCols.find(_.getName == "id").head.getValue
                val afterCheadLine = afterCols.find(_.getName == "cheadline").head.getValue
                val afterTag = afterCols.find(_.getName == "tag").head.getValue
                val afterShortLead = afterCols.find(_.getName == "cshortleadbody").head.getValue
                val afterLongLead = afterCols.find(_.getName == "clongleadbody").head.getValue
                val pubDate = afterCols.find(_.getName == "last_publish_time").head.getValue

                val cLead = if(afterLongLead.length > afterShortLead.length) afterLongLead else afterShortLead

                log.info("UPDATE: ---->>> before: id[{}] - cheadline[{}] - tag [{}]", beforeId, beforeCheadLine, beforeTag)
                log.info("UPDATE: ---->>> after: id[{}] - cheadline[{}] - tag [{}]", afterId, afterCheadLine, afterTag)

                if (afterId != "") {

                    // Update tag
                    if (beforeTag != afterTag) {
                        log.info("The tag was changed, start to update tag.")

                        // Delete old data first
                        delProperties(_contentType, afterId)

                        // Then insert new data
                        val storyData = afterTag.split(",").filter(_ != "").distinct.map(tag => (tag.trim, afterId, afterCheadLine, cLead, pubDate))
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
                        if (beforeShortLead != afterShortLead || beforeLongLead != afterLongLead) {
                            log.info("The lead was changed, start to update lead.")
                            updateLead(afterId, _contentType, cLead)
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

            // Keep total items start
            dataList.foreach(x => {
                val tag = x._1
                val totalSql = "SELECT contentid FROM tag_content where tag='%s' and `type`=%d order by pubdate desc, contentid desc limit %d, 10;".format(tag, _contentType, keepTotalNum)

                val mPs = conn.prepareStatement(totalSql)
                val mRs = mPs.executeQuery(totalSql)

                var contentIds = Set[String]()
                while (mRs.next()){
                    contentIds = contentIds + mRs.getString(1)
                }

                mRs.close()
                mPs.close()

                if(contentIds.nonEmpty) {
                    val delSql = "DELETE FROM tag_content where tag='%s' and `type`=%d and contentid in (%s);".format(tag, _contentType, contentIds.mkString("'", "','", "'"))
                    val nPs = conn.prepareStatement(delSql)
                    val nRs = nPs.executeUpdate()

                    if(nRs > 0)
                        log.info("--->>For keeping total items, [%d] records delete successful >>>--::::".format(nRs))

                    nPs.close()
                }
            })
            // Keep total items end

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

    def updateLead(contentId: String, contentType: Int, lead: String): Boolean ={
        var ret = false

        try {

            if (mysql_driver == null) {
                throw new Exception("Didn't initialize mysql driver.")
            }

            if(contentId == "") {
                throw new Exception("Must specify a story id.")
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

}
