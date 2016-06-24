package com.wanbo.channel

import com.wanbo.lib.Content

/**
  * Created by wanbo on 16/6/20.
  */
abstract class Cms2HBase {
    def addProperties(content: Content)
    def delProperties(contentId: String): Boolean
}
