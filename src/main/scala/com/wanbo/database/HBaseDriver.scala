package com.wanbo.database

import java.util.Properties

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.slf4j.LoggerFactory

/**
 * HBase driver.
 * Created by wanbo on 15/4/17.
// */
case class HBaseDriver() extends Driver {
    private var db_zq: String = ""
    private var _conn: Connection = null

    private val log = LoggerFactory.getLogger(classOf[HBaseDriver])

    override def setConfiguration(conf: Properties): Unit = {

        db_zq = conf.getOrDefault("hbase.host", "localhost").toString

        if(db_zq == "")
            log.warn("The Zookeeper host for HBase is not found ---------- !!!")
    }

    def getConnector(dbName: String = "test", writable: Boolean = false): Connection ={

        try {

            if(db_zq != ""){
                val hConf = HBaseConfiguration.create()
                hConf.set("hbase.zookeeper.quorum", db_zq)
                hConf.set("hbase.client.operation.timeout", "30000")
                hConf.set("hbase.client.retries.number", "3")

                _conn = ConnectionFactory.createConnection(hConf)
            } else {
                throw new Exception("The configuration option [hbase.zookeeper.quorum] is empty!")
            }

        } catch {
            case e: Exception =>
                throw e
        }

        _conn
    }

    protected def close(): Unit ={
        try{
            if(_conn != null)
                _conn.close()
        } catch {
            case e: Exception =>
        }
    }
}
