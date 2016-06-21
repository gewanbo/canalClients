package com.wanbo.database

import com.wanbo.utils.Logging

/**
  * Created by wanbo on 16/6/20.
  */
object DriverPool extends Logging {

    private var _driverPoolList: List[Driver] = List[Driver]()

    /**
      * Initialize all available data source.
      *
      * Called by manager when it start up.
      */
    def initialize(settings: List[Map[String, String]]): Unit ={

        log.info("Initialize Data Source.")

    }

    def appendDriver(driver: Driver): Unit ={
        _driverPoolList = _driverPoolList :+ driver
    }

    /**
      * Get a special driver from driver pool.
      *
      * @param t                The type of driver, Mysql or HBase or others.
      * @param isWritable       Whether the database can writable or not.
      * @return                 Option with driver.
      */
    def getDriver(t: String, isWritable: Boolean): Option[Driver] = {
        var driver: Option[Driver] = None

        t match {
            case "mysql" =>
                driver = Some(_driverPoolList.filter(_.isInstanceOf[MysqlDriver]).head)
            case "hbase" =>
                driver = Some(_driverPoolList.filter(_.isInstanceOf[HBaseDriver]).head)
            case _ =>
                // Ignore
        }

        driver
    }
}
