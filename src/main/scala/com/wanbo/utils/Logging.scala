package com.wanbo.utils

import org.slf4j.LoggerFactory

/**
 * Trait of log data.
 * Created by wanbo on 15/8/26.
 */
trait Logging {
    protected val log = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
}
