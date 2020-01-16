/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xixici.flink

import org.apache.flink.api.scala._

/**
  * Skeleton for a Flink Batch Job.
  *
  * For a tutorial how to write a Flink batch application, check the
  * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
  *
  * To package your application into a JAR file for execution,
  * change the main class in the POM.xml file to this class (simply search for 'mainClass')
  * and run 'mvn clean package' on the command line.
  */
object BatchJob {


  def main(args: Array[String]): Unit = {
    val benv = ExecutionEnvironment.getExecutionEnvironment
    val dataSet = benv.readTextFile("C:\\Soft\\flink-1.9.1\\NOTICE")
    dataSet.flatMap {
      _.toLowerCase.split(" ")
    }
      .filter(_.nonEmpty)
      .map {
        (_, 1)
      }
      .groupBy(0)
      .sum(1)
      .print()
  }

}
