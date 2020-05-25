/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import java.util.Properties

/**
  * A set of tasks submitted together to the low-level TaskScheduler, usually representing
  * missing partitions of a particular stage.
  */
private[spark] class TaskSet(
                            //TaskSet锁包含的Task的数组
                              val tasks: Array[Task[_]],
                            //Task所属的Stage的身份标识
                              val stageId: Int,
                            //Stage尝试的身份标识
                              val stageAttemptId: Int,
                            //优先级。通常以JobId作为优先级
                              val priority: Int,
                            //包含了与Job有个的调度， Job group、 描述等属性的Properties
                              val properties: Properties) {

  //TaskSet的身份标识
  val id: String = stageId + "." + stageAttemptId

  override def toString: String = "TaskSet " + id
}
