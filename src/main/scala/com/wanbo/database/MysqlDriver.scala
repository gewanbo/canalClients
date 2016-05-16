package com.wanbo.database

import java.sql.{Connection, SQLException}
import java.util.Properties

import com.wanbo.utils.Logging
import org.springframework.jdbc.datasource.DriverManagerDataSource

/**
 * Mysql driver
 * Created by wanbo on 15/4/16.
 */
case class MysqlDriver() extends Driver {

    private var _conn:Connection = null

    override def setConfiguration(conf: Properties): Unit = {

    }

    def getConnector(dbName: String = "test", writable: Boolean = false): Connection ={

        try {

            val sourceList = MysqlDriver.dataSourceList.filter(x => x._1._1 == dbName && x._1._2 == writable)

            if(sourceList.nonEmpty) {
                _conn = util.Random.shuffle(sourceList).head._2.getConnection
            } else {
                throw new Exception("Didn't find the available database source.")
            }

        } catch {
            case sqlE: SQLException =>
                throw new Exception("40001")
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

object MysqlDriver extends Logging {

    private var dataSourceList: List[((String, Boolean), DriverManagerDataSource)] = List[((String, Boolean), DriverManagerDataSource)]()

    /**
     * Initialize all available data source.
     *
     * Called by manager when it start up.
     */
    def initializeDataSource(settings: List[Map[String, String]]): Unit ={

        log.info("Initialize Data Source.")

        if (settings.nonEmpty) {
            settings.foreach(x => {
                val db_host = x.getOrElse("host", "")
                val db_port = x.getOrElse("port", "")
                val db_username = x.getOrElse("uname", "")
                val db_password = x.getOrElse("upswd", "")
                val db_name = x.getOrElse("dbname", "")
                val db_writable = if (x.get("writable").get.toLowerCase == "true") true else false

                val ds: DriverManagerDataSource = new DriverManagerDataSource()
                ds.setUrl("jdbc:mysql://%s:%s/%s?characterEncoding=utf-8".format(db_host, db_port, db_name))
                ds.setUsername(db_username)
                ds.setPassword(db_password)

                MysqlDriver.dataSourceList = MysqlDriver.dataSourceList :+ ((db_name, db_writable), ds)
            })
        }

    }

}