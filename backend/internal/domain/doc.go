// Package domain 是领域层根包。
//
// 依赖规则：
//  1. domain 只允许依赖 Go 标准库和领域内部包；
//  2. 禁止导入 infrastructure、application、interfaces；
//  3. 实体只包含业务行为与业务规则，不包含 GORM 标签、SQL、Redis、MQ 等实现细节；
//  4. 仓储以接口形式定义在 domain，具体实现放在 infrastructure。
//
// 目录约定：
//
//	domain/account  账号与认证领域
//	domain/video    视频、点赞、评论领域
//	domain/social   关注关系领域
//	domain/feed     Feed 时间线与推荐领域
//
// 存量代码目前按业务模块平铺，后续按限界上下文逐步迁移到该分层。
package domain
