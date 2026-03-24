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

import javax.crypto.SecretKey

import io.jsonwebtoken.{JwtException, Jwts}
import io.jsonwebtoken.io.Decoders
import io.jsonwebtoken.security.Keys
import jakarta.servlet.{Filter, FilterChain, FilterConfig, ServletRequest, ServletResponse}
import jakarta.servlet.http.{HttpServletRequest, HttpServletResponse}

/**
 * Spark Web UI JWT验证过滤器，要求请求头中携带经过JWS签名的JSON Web Token
 * 
 * 使用该过滤器需要在Spark配置中指定以下参数：
 * {{{
 *   - spark.ui.filters=org.apache.spark.ui.JWSFilter
 *   - spark.org.apache.spark.ui.JWSFilter.param.secretKey=BASE64URL-ENCODED-YOUR-PROVIDED-KEY
 * }}}
 * 客户端请求需要携带 {@code Authorization: Bearer <jws>} 请求头
 * {{{
 *   - <jws> 格式为三部分：'<header>.<payload>.<signature>'
 *   - <header> 是base64url编码的 '{"alg":"HS256","typ":"JWT"}'
 *   - <payload> 是用户自定义内容的base64url编码，内容不做校验
 *   - <signature> 是使用用户提供密钥对'<header>.<payload>'生成的签名
 * }}}
 * 
 * 本过滤器用于为Spark Web UI增加JWT身份认证能力，只有携带合法签名JWT的请求才能访问UI
 */
private class JWSFilter extends Filter {
  private val AUTHORIZATION = "Authorization"

  private var key: SecretKey = null

  /**
   * 初始化过滤器，从配置加载并验证JWT签名密钥
   * 
   * 如果未提供secretKey参数会抛出IllegalArgumentException
   * 如果提供的密钥强度不足会抛出WeakKeyException
   * 
   * @param config 过滤器配置对象
   */
  override def init(config: FilterConfig): Unit = {
    // 从初始化参数读取Base64URL编码的密钥，转换为HMAC-SHA密钥对象
    key = Keys.hmacShaKeyFor(Decoders.BASE64URL.decode(config.getInitParameter("secretKey")));
  }

  /**
   * 执行过滤逻辑，验证请求中的JWT令牌，合法则放行，非法则返回403错误
   * 
   * @param req Servlet请求对象
   * @param res Servlet响应对象
   * @param chain 过滤器链
   */
  override def doFilter(req: ServletRequest, res: ServletResponse, chain: FilterChain): Unit = {
    // 转换为HTTP请求/响应对象
    val hreq = req.asInstanceOf[HttpServletRequest]
    val hres = res.asInstanceOf[HttpServletResponse]
    // 禁用缓存，防止敏感UI内容被浏览器缓存
    hres.setHeader("Cache-Control", "no-cache, no-store, must-revalidate")

    try {
      // 获取Authorization请求头
      val header = hreq.getHeader(AUTHORIZATION)
      header match {
        // 请求头不存在，返回403禁止访问
        case null =>
          hres.sendError(HttpServletResponse.SC_FORBIDDEN, s"${AUTHORIZATION} header is missing.")
        // 匹配Bearer格式，提取JWT令牌
        case s"Bearer $token" =>
          // 验证JWT签名，验证失败会抛出JwtException
          val claims = Jwts.parser().verifyWith(key).build().parseSignedClaims(token)
          // 验证通过，放行请求到下一个过滤器
          chain.doFilter(req, res)
        // Authorization头格式不正确，返回403
        case _ =>
          hres.sendError(HttpServletResponse.SC_FORBIDDEN, s"Malformed ${AUTHORIZATION} header.")
      }
    } catch {
      // JWT验证失败（签名无效、过期等），不暴露具体错误细节，统一返回403
      case e: JwtException =>
        hres.sendError(HttpServletResponse.SC_FORBIDDEN, "JWT Validate Fail")
    }
  }
}