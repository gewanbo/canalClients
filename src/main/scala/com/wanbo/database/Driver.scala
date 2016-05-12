package com.wanbo.database

import java.util.Properties

/**
 * Database driver
 * Created by wanbo on 15/4/16.
 */
trait Driver {
    def setConfiguration(conf: Properties)
}
