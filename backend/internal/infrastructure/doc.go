// Package infrastructure 是基础设施层根包。
//
// 职责：
//  1. 实现 domain 定义的仓储接口；
//  2. 封装 MySQL、Redis、RabbitMQ、WebSocket 等外部依赖；
//  3. 负责 GORM 模型与领域实体的相互转换。
//
// 依赖规则：infrastructure 可以依赖 domain，但禁止依赖 interfaces。
package infrastructure
