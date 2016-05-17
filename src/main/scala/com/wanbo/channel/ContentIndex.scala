package com.wanbo.channel

/**
  * Created by wanbo on 16/5/17.
  */
abstract class ContentIndex {

    /**
      * 1: Story  2: Video  3: Interactive  4: Photonews
      */
    protected var _contentType = 0

    def init()
    def addProperties(contentType: Int, dataList: Array[(String, String, String)])
    def delProperties(contentType: Int, contentId: String): Boolean
    def updateCheadLine(contentId: String, contentType: Int, cheadLine: String): Boolean
}
