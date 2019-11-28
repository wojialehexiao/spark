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

package org.apache.spark.ui

import java.util.EnumSet

import javax.servlet.DispatcherType
import javax.servlet.http.{HttpServlet, HttpServletRequest}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.Utils
import org.apache.spark.{SSLOptions, SecurityManager, SparkConf}
import org.eclipse.jetty.servlet.{FilterHolder, FilterMapping, ServletContextHandler, ServletHolder}
import org.json4s.JsonAST.{JNothing, JValue}

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.xml.Node

/**
  * 包含服务器的UI层次结构的顶级组件。
  *
  * 每个WebUI代表选项卡的集合，每个选项卡又代表页面的集合。
  * 但是，选项卡的使用是可选的。 WebUI可以选择直接包含页面。
  */
private[spark] abstract class WebUI(
                                     val securityManager: SecurityManager,
                                     val sslOptions: SSLOptions,
                                     port: Int,
                                     conf: SparkConf,
                                     basePath: String = "",
                                     name: String = "")
  extends Logging {

  /**
    * WebUI tab的缓冲数组
    */
  protected val tabs = ArrayBuffer[WebUITab]()

  /**
    * ServletContextHandler的缓冲数据。ServletContextHandler是Jetty提供的API， 负责对ServletContext进行处理
    */
  protected val handlers = ArrayBuffer[ServletContextHandler]()

  /**
    * WebUIPage与ServletContextHandler缓冲数组之间的映射关系。
    * 由于WebUIPage的两个方法render和renderJson分别需要由一个对应的ServletContextHandler处理，
    * 所以WebUIPage对应两个ServletContextHandler
    */
  protected val pageToHandlers = new HashMap[WebUIPage, ArrayBuffer[ServletContextHandler]]

  /**
    * 用户缓冲WebUI的jetty的服务器信息
    */
  protected var serverInfo: Option[ServerInfo] = None

  /**
    * 当前WebUI的jetty服务的主机名。优先使用环境变量SPARK_PUBLIC_DNS，
    * 否则使用spark.driver.host属性指定的， 当两个都没有时，将默认使用工具类Utils的localHostName方法
    */
  protected val publicHostName = Option(conf.getenv("SPARK_PUBLIC_DNS")).getOrElse(
    conf.get(DRIVER_HOST_ADDRESS))

  /**
    * 过滤了$符号的简单类名。
    */
  private val className = Utils.getFormattedClassName(this)

  /**
    * 获取basePath
    * @return
    */
  def getBasePath: String = basePath


  /**
    * 获取所有的WebUITab
    * @return
    */
  def getTabs: Seq[WebUITab] = tabs

  /**
    * 获取所有的ServletContextHandler
    * @return
    */
  def getHandlers: Seq[ServletContextHandler] = handlers

  def getDelegatingHandlers: Seq[DelegatingServletContextHandler] = {
    handlers.map(new DelegatingServletContextHandler(_))
  }

  /** 将选项卡及其所有附加页面附加到此UI。 */
  def attachTab(tab: WebUITab): Unit = {
    tab.pages.foreach(attachPage)
    tabs += tab
  }

  /** 从此UI分离选项卡及其所有附加页面。 */
  def detachTab(tab: WebUITab): Unit = {
    tab.pages.foreach(detachPage)
    tabs -= tab
  }

  /** 从此UI分离页面及其所有附加的处理程序。 */
  def detachPage(page: WebUIPage): Unit = {
    pageToHandlers.remove(page).foreach(_.foreach(detachHandler))
  }

  /** 将页面附加到此UI。*/
  def attachPage(page: WebUIPage): Unit = {
    val pagePath = "/" + page.prefix
    val renderHandler = createServletHandler(pagePath,
      (request: HttpServletRequest) => page.render(request), conf, basePath)
    val renderJsonHandler = createServletHandler(pagePath.stripSuffix("/") + "/json",
      (request: HttpServletRequest) => page.renderJson(request), conf, basePath)
    attachHandler(renderHandler)
    attachHandler(renderJsonHandler)
    val handlers = pageToHandlers.getOrElseUpdate(page, ArrayBuffer[ServletContextHandler]())
    handlers += renderHandler
    handlers += renderJsonHandler
  }

  /** 将处理程序附加到此UI。 */
  def attachHandler(handler: ServletContextHandler): Unit = synchronized {
    handlers += handler
    serverInfo.foreach(_.addHandler(handler, securityManager))
  }

  /** 将处理器附加到此UI。 */
  def attachHandler(contextPath: String, httpServlet: HttpServlet, pathSpec: String): Unit = {
    val ctx = new ServletContextHandler()
    ctx.setContextPath(contextPath)
    ctx.addServlet(new ServletHolder(httpServlet), pathSpec)
    attachHandler(ctx)
  }

  /** 从此UI分离处理器。*/
  def detachHandler(handler: ServletContextHandler): Unit = synchronized {
    handlers -= handler
    serverInfo.foreach(_.removeHandler(handler))
  }

  /**
    * Detaches the content handler at `path` URI.
    *
    * @param path Path in UI to unmount.
    */
  def detachHandler(path: String): Unit = {
    handlers.find(_.getContextPath() == path).foreach(detachHandler)
  }

  /**
    * 添加静态内容处理器。
    * 创建今天文件服务的 ServletContextHandler
    *
    * @param resourceBase Root of where to find resources to serve.
    * @param path         Path in UI where to mount the resources.
    */
  def addStaticHandler(resourceBase: String, path: String = "/static"): Unit = {
    attachHandler(JettyUtils.createStaticHandler(resourceBase, path))
  }

  /** 用于初始化UI组件的钩子 */
  def initialize(): Unit

  /** 绑定到此Web界面后面的HTTP服务器。*/
  def bind(): Unit = {
    assert(serverInfo.isEmpty, s"Attempted to bind $className more than once!")
    try {
      val host = Option(conf.getenv("SPARK_LOCAL_IP")).getOrElse("0.0.0.0")
      val server = startJettyServer(host, port, sslOptions, conf, name)
      handlers.foreach(server.addHandler(_, securityManager))
      serverInfo = Some(server)
      logInfo(s"Bound $className to $host, and started at $webUrl")
    } catch {
      case e: Exception =>
        logError(s"Failed to bind $className", e)
        System.exit(1)
    }
  }

  /** @return Whether SSL enabled. Only valid after [[bind]]. */
  def isSecure: Boolean = serverInfo.map(_.securePort.isDefined).getOrElse(false)

  /** @return The scheme of web interface. Only valid after [[bind]]. */
  def scheme: String = if (isSecure) "https://" else "http://"

  /** @return Web界面的URL。 Only valid after [[bind]]. */
  def webUrl: String = s"${scheme}$publicHostName:${boundPort}"

  /** @return 该服务器绑定到的实际端口。 Only valid after [[bind]]. */
  def boundPort: Int = serverInfo.map(si => si.securePort.getOrElse(si.boundPort)).getOrElse(-1)

  /** 将服务器停止在此Web界面后面。 Only valid after [[bind]]. */
  def stop(): Unit = {
    assert(serverInfo.isDefined,
      s"Attempted to stop $className before binding to a server!")
    serverInfo.foreach(_.stop())
  }
}


/**
  * 代表页面集合的选项卡。
  * 前缀将附加到父地址以形成完整路径，并且不得包含斜杠。
  */
private[spark] abstract class WebUITab(parent: WebUI, val prefix: String) {
  //当前WebUiTab所包含的WebUiPage的缓冲数组
  val pages = ArrayBuffer[WebUIPage]()
  //当前WebUITab的名称， name实际是prefix首字母大写后取得
  val name = prefix.capitalize

  /** 首先将当前WebUITab的前缀与WebUIPage的前缀拼接，作为WebUIPage的访问路径，然后想page添加WebUIPage */
  def attachPage(page: WebUIPage): Unit = {
    page.prefix = (prefix + "/" + page.prefix).stripSuffix("/")
    pages += page
  }

  /** 从父UI获取标题选项卡的列表。 */
  def headerTabs: Seq[WebUITab] = parent.getTabs

  //获取父WebUI的基本路径
  def basePath: String = parent.getBasePath
}


/**
  * 表示UI层次结构中的叶节点的页面。
  *
  * 未指定WebUIPage的直接父级，因为它可以是WebUI或WebUITab。
  * 如果父级是WebUI，则将前缀附加到父级的地址以形成完整路径。
  * 否则，如果父级是WebUITab，则将前缀附加到父级的超级前缀以形成相对路径。前缀不能包含斜杠。
  */
private[spark] abstract class WebUIPage(var prefix: String) {
  //渲染页面
  def render(request: HttpServletRequest): Seq[Node]

  //生成json
  def renderJson(request: HttpServletRequest): JValue = JNothing
}

private[spark] class DelegatingServletContextHandler(handler: ServletContextHandler) {

  def prependFilterMapping(
                            filterName: String,
                            spec: String,
                            types: EnumSet[DispatcherType]): Unit = {
    val mapping = new FilterMapping()
    mapping.setFilterName(filterName)
    mapping.setPathSpec(spec)
    mapping.setDispatcherTypes(types)
    handler.getServletHandler.prependFilterMapping(mapping)
  }

  def addFilter(
                 filterName: String,
                 className: String,
                 filterParams: Map[String, String]): Unit = {
    val filterHolder = new FilterHolder()
    filterHolder.setName(filterName)
    filterHolder.setClassName(className)
    filterParams.foreach { case (k, v) => filterHolder.setInitParameter(k, v) }
    handler.getServletHandler.addFilter(filterHolder)
  }

  def filterCount(): Int = {
    handler.getServletHandler.getFilters.length
  }

  def getContextPath(): String = {
    handler.getContextPath
  }
}
