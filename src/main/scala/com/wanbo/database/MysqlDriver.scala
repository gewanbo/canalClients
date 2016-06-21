package com.wanbo.database

import java.sql.{Connection, SQLException}
import java.util.Properties

import org.springframework.jdbc.datasource.DriverManagerDataSource

/**
 * Mysql driver
 * Created by wanbo on 15/4/16.
 */
case class MysqlDriver() extends Driver {

    private var _conn:Connection = null

    private var db_host = ""
    private var db_port = ""
    private var db_name = ""
    private var db_username = ""
    private var db_password = ""

    override def setConfiguration(conf: Properties): Unit = {
        db_host = conf.getProperty("host")
        db_port = conf.getProperty("port")
        db_name = conf.getProperty("dbname")
        db_username = conf.getProperty("uname")
        db_password = conf.getProperty("upswd")
    }

    def getConnector(dbName: String = "test", writable: Boolean = false): Connection ={

        try {

            val ds: DriverManagerDataSource = new DriverManagerDataSource()
            ds.setUrl("jdbc:mysql://%s:%s/%s?characterEncoding=utf-8".format(db_host, db_port, db_name))
            ds.setUsername(db_username)
            ds.setPassword(db_password)

            _conn = ds.getConnection

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