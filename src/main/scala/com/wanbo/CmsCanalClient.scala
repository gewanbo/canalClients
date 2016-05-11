package com.wanbo

import java.lang.Thread.UncaughtExceptionHandler

import com.alibaba.otter.canal.client.CanalConnector
import org.apache.commons.lang.SystemUtils
import org.springframework.util.Assert

/**
  * Created by wanbo on 16/5/11.
  */
class CmsCanalClient {
    val SEP: String               = SystemUtils.LINE_SEPARATOR
    val DATE_FORMAT: String       = "yyyy-MM-dd HH:mm:ss"
    val running                   = false
    val handler                   = new UncaughtExceptionHandler {
        override def uncaughtException(t: Thread, e: Throwable): Unit = {
            // log
        }
    }
    var thread: Thread = null
    val connector: CanalConnector      = null
    var context_format: String = null
    val row_format: String             = _
    val transaction_format: String     = ""
    val destination: String            = ""

    context_format = SEP + "****************************************************" + SEP
    context_format += "* Batch Id: [{}] ,count : [{}] , memsize : [{}] , Time : {}" + SEP



    def start(): Unit = {
        Assert.notNull(connector, "connector is null")
        thread = new Thread(new Runnable {
            override def run(): Unit = {
                // process
            }
        })
    }
}
