package com.wanbo

import java.io.FileInputStream
import java.net.InetSocketAddress
import java.util.Properties

import com.alibaba.otter.canal.client.CanalConnectors
import com.wanbo.utils.Logging

/**
  * Created by wanbo on 16/5/12.
  */
object CmsApp extends CmsCanalClient with Logging {

    def main(args: Array[String]) {

        try {
            val config = new Properties()

            log.info("Start to load config file.")
            val configFile = System.getProperty("easy.conf")
            log.info("Config file:" + configFile)

            val is = new FileInputStream(configFile)
            config.loadFromXML(is)
            is.close()

            log.info("Config file load successful!")

            val canalServer = config.getProperty("canal.server")
            val canalPort = config.getProperty("canal.port").toInt
            val destination = config.getProperty("canal.instance")

            log.info("canal: server-{} port-{} instance-{}", canalServer, canalPort.toString, destination)

            val connector = CanalConnectors.newSingleConnector(new InetSocketAddress(canalServer, canalPort), destination, "", "")

            val cmsClient = new CmsCanalClient(destination)
            cmsClient.setConnector(connector)
            cmsClient.start()

            Runtime.getRuntime.addShutdownHook(new Thread{
                override def run(): Unit ={
                    try {
                        cmsClient.stop()
                    } catch {
                        case e: Exception =>
                            log.warn("Something goes wrong when stopping canal:", e)
                    } finally {
                        log.info("### Cannal client is down. ###")
                    }
                }
            })

        } catch {
            case e: Exception =>
                log.error("Error:", e)
        }

    }

}
