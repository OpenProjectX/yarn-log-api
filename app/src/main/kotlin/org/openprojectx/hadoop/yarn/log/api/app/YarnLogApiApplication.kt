package org.openprojectx.hadoop.yarn.log.api.app

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication


@SpringBootApplication
class YarnLogApiApplication

fun main(args: Array<String>) {

    runApplication<YarnLogApiApplication>(*args)
}