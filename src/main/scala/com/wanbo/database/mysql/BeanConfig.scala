package com.wanbo.database.mysql

import com.wanbo.database.MysqlDriver
import org.springframework.context.annotation.{Bean, Configuration}

/**
 * Bean configure
 * Created by wanbo on 15/4/16.
 */
@Configuration
class BeanConfig {
    @Bean
    def DataSource(): DataSource = {new DataSource()}
    @Bean
    def MysqlDriver(): MysqlDriver = {new MysqlDriver()}
}
