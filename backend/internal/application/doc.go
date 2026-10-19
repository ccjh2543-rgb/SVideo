// Package application 是应用层根包。
//
// 职责：
//  1. 编排用例流程，例如登录、发布视频、点赞、拉取 Feed；
//  2. 调用 domain 的实体行为与仓储接口；
//  3. 处理事务边界、幂等、缓存、消息投递等应用级协调；
//  4. 不直接依赖 Gin、GORM、Redis、RabbitMQ 的具体实现。
//
// 依赖规则：application 可以依赖 domain，但禁止依赖 interfaces 和 infrastructure。
package application
