package com.wanbo.pipeline

import com.alibaba.otter.canal.protocol.CanalEntry.RowChange
import com.wanbo.database.MysqlDriver

/**
  * Created by wanbo on 16/5/17.
  */
trait Pipeline {
    def setDriver(driver: MysqlDriver)
    def process(changedRow: RowChange)
}
