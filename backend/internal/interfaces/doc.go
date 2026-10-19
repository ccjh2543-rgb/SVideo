// Package interfaces 是接口层根包。
//
// 职责：
//  1. 暴露 HTTP、WebSocket 等对外入口；
//  2. 负责请求参数校验、鉴权中间件、响应组装；
//  3. 将请求转换为 application 层的用例调用。
//
// 依赖规则：interfaces 可以依赖 application，但禁止直接依赖 infrastructure。
package interfaces
