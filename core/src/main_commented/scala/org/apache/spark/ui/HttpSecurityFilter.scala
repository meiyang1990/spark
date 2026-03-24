// 这个文件已经全部加上中文注释
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

import java.util.{Enumeration, Map => JMap}

import scala.jdk.CollectionConverters._

import jakarta.servlet._
import jakarta.servlet.http.{HttpServletRequest, HttpServletRequestWrapper, HttpServletResponse}
import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.config.UI._

/**
 * Spark Web UI的HTTP安全防护过滤器，为所有Web请求提供安全防护，主要功能包括：
 * 1. 身份认证请求的访问权限控制
 * 2. 对请求数据进行XSS攻击内容检测与清理
 * 3. 设置安全响应头防范各类常见Web攻击
 * 
 * 对所有请求参数进行清理，转义HTML内容并移除危险内容。
 */
private class HttpSecurityFilter(
    conf: SparkConf,
    securityMgr: SecurityManager) extends Filter {

  /**
   * 过滤器核心处理方法，对 incoming 请求执行安全检查与防护
   * @param req 原始Servlet请求
   * @param res Servlet响应
   * @param chain 过滤器链
   */
  override def doFilter(req: ServletRequest, res: ServletResponse, chain: FilterChain): Unit = {
    val hreq = req.asInstanceOf[HttpServletRequest]
    val hres = res.asInstanceOf[HttpServletResponse]
    // 设置缓存控制头，禁止浏览器缓存响应内容
    hres.setHeader("Cache-Control", "no-cache, no-store, must-revalidate")

    // 生成CSP头部使用的随机nonce值，防止内联脚本执行
    val cspNonce = CspNonce.generate()
    try {
      // 设置内容安全策略(CSP)头，限制资源加载来源，防范XSS攻击
      hres.setHeader("Content-Security-Policy",
        s"default-src 'self'; script-src 'self' 'nonce-$cspNonce'; " +
        s"style-src 'self' 'unsafe-inline'; img-src 'self' data:; " +
        s"object-src 'none'; base-uri 'self';")

      // 获取请求认证的用户名
      val requestUser = hreq.getRemoteUser()

      // 处理doAs参数，支持代理服务器(如Knox)用户伪装功能：仅管理员允许伪装其他用户
      val effectiveUser = Option(hreq.getParameter("doAs"))
        .map { proxy =>
          // 非管理员伪装其他用户时，返回403禁止访问
          if (requestUser != proxy && !securityMgr.checkAdminPermissions(requestUser)) {
            hres.sendError(HttpServletResponse.SC_FORBIDDEN,
              s"User $requestUser is not allowed to impersonate others.")
            return
          }
          proxy
        }
        .getOrElse(requestUser)

      // 检查当前用户是否拥有Web UI页面查看权限，无权限则返回403
      if (!securityMgr.checkUIViewPermissions(effectiveUser)) {
        hres.sendError(HttpServletResponse.SC_FORBIDDEN,
          s"User $effectiveUser is not authorized to access this page.")
        return
      }

      // 配置X-Frame-Options头，防止点击劫持攻击，默认仅允许同源域名嵌入，支持配置指定允许的域名
      val xFrameOptionsValue = conf.getOption("spark.ui.allowFramingFrom")
        .map { uri => s"ALLOW-FROM $uri" }
        .getOrElse("SAMEORIGIN")

      // 设置各类安全响应头
      hres.setHeader("X-Frame-Options", xFrameOptionsValue)
      hres.setHeader("X-XSS-Protection", conf.get(UI_X_XSS_PROTECTION))
      // 启用X-Content-Type-Options头，防止MIME类型嗅探篡改
      if (conf.get(UI_X_CONTENT_TYPE_OPTIONS)) {
        hres.setHeader("X-Content-Type-Options", "nosniff")
      }
      // HTTPS请求配置严格传输安全(HSTS)头
      if (hreq.getScheme() == "https") {
        conf.get(UI_STRICT_TRANSPORT_SECURITY).foreach(
          hres.setHeader("Strict-Transport-Security", _))
      }

      // 将清理后的请求传入过滤器链继续处理
      chain.doFilter(new XssSafeRequest(hreq, effectiveUser), res)
    } finally {
      // 清理当前线程的CSP nonce状态
      CspNonce.clear()
    }
  }

}

/**
 * 封装HttpServletRequest，对所有请求参数进行XSS防护清理，同时替换当前用户名支持用户伪装
 * 通过覆盖所有参数获取方法，返回经过XSS清理后的参数内容，防止注入攻击
 */
private class XssSafeRequest(req: HttpServletRequest, effectiveUser: String)
  extends HttpServletRequestWrapper(req) {

  // 匹配换行、单引号及其URL编码形式的正则表达式，用于提前清理危险字符
  private val NEWLINE_AND_SINGLE_QUOTE_REGEX = raw"(?i)(\r\n|\n|\r|%0D%0A|%0A|%0D|'|%27)".r

  // 预清理所有请求参数和参数名，缓存清理后的结果
  private val parameterMap: Map[String, Array[String]] = {
    super.getParameterMap().asScala.map { case (name, values) =>
      stripXSS(name) -> values.map(stripXSS)
    }.toMap
  }

  // 返回伪装后的有效用户名
  override def getRemoteUser(): String = effectiveUser

  // 返回清理后的参数Map
  override def getParameterMap(): JMap[String, Array[String]] = parameterMap.asJava

  // 返回清理后的参数名称枚举
  override def getParameterNames(): Enumeration[String] = {
    parameterMap.keys.iterator.asJavaEnumeration
  }

  // 返回指定名称清理后的参数值数组
  override def getParameterValues(name: String): Array[String] = parameterMap.get(name).orNull

  // 返回指定名称清理后的单个参数值
  override def getParameter(name: String): String = {
    parameterMap.get(name).flatMap(_.headOption).orNull
  }

  /**
   * 清理字符串中的XSS危险内容，先移除换行和单引号，再转义HTML特殊字符
   * @param str 原始输入字符串
   * @return 清理后的安全字符串
   */
  private def stripXSS(str: String): String = {
    if (str != null) {
      // 移除换行与单引号，然后转义HTML4特殊字符，防止XSS注入
      StringEscapeUtils.escapeHtml4(NEWLINE_AND_SINGLE_QUOTE_REGEX.replaceAllIn(str, ""))
    } else {
      null
    }
  }

}