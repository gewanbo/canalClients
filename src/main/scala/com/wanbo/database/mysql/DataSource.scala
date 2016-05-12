package com.wanbo.database.mysql

import java.util.Properties

import org.springframework.jdbc.datasource.DriverManagerDataSource

/**
 * Data Source class
 * Created by wanbo on 15/4/16.
 */
class DataSource extends DriverManagerDataSource {
    def DataSource(): Unit ={
        val prop = new Properties()
        prop.put("connectionProperties", "characterEncoding=utf-8")
        this.setDriverClassName("org.springframework.jdbc.datasource.DriverManagerDataSource")
        this.setConnectionProperties(prop)
    }
}
