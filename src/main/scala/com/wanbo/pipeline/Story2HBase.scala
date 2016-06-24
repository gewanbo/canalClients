package com.wanbo.pipeline

import com.alibaba.otter.canal.protocol.CanalEntry.{EventType, RowChange}
import com.wanbo.channel.Cms2HBase
import com.wanbo.database.{DriverPool, HBaseDriver}
import com.wanbo.lib.{Content, Story}
import com.wanbo.utils.Logging
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Delete, Put}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._

/**
  * Created by wanbo on 16/6/23.
  */
class Story2HBase extends Cms2HBase with Pipeline with Logging {

    private val db_driver: HBaseDriver = DriverPool.getDriver("hbase", isWritable = true).orNull.asInstanceOf[HBaseDriver]

    init()

    def init(): Unit ={

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


                log.info("INSERT: ---->>> after: id[{}] - cheadline[{}] - {}", itemId, itemCheadLine, "")

                if(itemId != "" && itemCheadLine != ""){
                    addProperties(Story(itemId, itemCheadLine))
                }

            } else if (eventType == EventType.DELETE) {
                val afterCols = data.getAfterColumnsList.asScala
                val itemId = afterCols.find(_.getName == "id").head.getValue
                val itemCheadLine = afterCols.find(_.getName == "cheadline").head.getValue

                log.info("DELETE: ---->>> after: id[{}] - cheadline[{}] - {}", itemId, itemCheadLine, "")

                if(itemId != "") {
                    delProperties(itemId)
                }

            } else if (eventType == EventType.UPDATE) {
                val beforeCols = data.getBeforeColumnsList.asScala
                val beforeId = beforeCols.find(_.getName == "id").head.getValue
                val beforeCheadLine = beforeCols.find(_.getName == "cheadline").head.getValue

                val afterCols = data.getAfterColumnsList.asScala
                val afterId = afterCols.find(_.getName == "id").head.getValue
                val afterCheadLine = afterCols.find(_.getName == "cheadline").head.getValue


                log.info("UPDATE: ---->>> before: id[{}] - cheadline[{}] - {}", beforeId, beforeCheadLine, "")
                log.info("UPDATE: ---->>> after: id[{}] - cheadline[{}] - {}", afterId, afterCheadLine, "")

                if (afterId != "" && beforeCheadLine != afterCheadLine) {
                    addProperties(Story(afterId, afterCheadLine))
                } else {
                    log.info("There is no necessary to update, the story data didn't chang.")
                }

            } else {
                // Ignore
            }

        })
    }

    def addProperties(content: Content): Unit ={
        try {
            val story = content.asInstanceOf[Story]

            if (db_driver == null) {
                throw new Exception("Didn't initialize mysql driver.")
            }

            val conn = db_driver.getConnector("", writable = true)

            val table = conn.getTable(TableName.valueOf("story_info"))

            val rowKey = story.id.reverse

            val p = new Put(Bytes.toBytes(rowKey))

            p.addColumn(Bytes.toBytes("m"), Bytes.toBytes("id"), Bytes.toBytes(story.id))
            p.addColumn(Bytes.toBytes("m"), Bytes.toBytes("cheadline"), Bytes.toBytes(story.cheadline))

            table.put(p)

            table.close()
            conn.close()
        } catch {
            case e: Exception =>
                log.error("There something error:", e)
        }
    }

    def delProperties(contentId: String): Boolean ={
        var ret = false

        try {

            if (db_driver == null) {
                throw new Exception("Didn't initialize mysql driver.")
            }

            if(contentId == "") {
                throw new Exception("Must specify a story id.")
            }

            val conn = db_driver.getConnector("", writable = true)

            val table = conn.getTable(TableName.valueOf("story_info"))

            val del = new Delete(Bytes.toBytes(contentId.reverse))
            table.delete(del)

            table.close()
            conn.close()

            ret = true
        } catch {
            case e: Exception =>
                log.error("There something error:", e)
        }

        ret
    }

}
