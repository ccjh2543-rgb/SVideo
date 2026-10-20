// Package social 是关注关系与创作者统计领域。
//
// 该包只包含实体、值对象、领域错误和仓储接口，不依赖 GORM、Redis、Gin。
package social

import "errors"

// 关注关系领域错误。
var (
	ErrInvalidRelation  = errors.New("invalid social relation")
	ErrCannotFollowSelf = errors.New("can not follow self")
	ErrAlreadyFollowed  = errors.New("already followed")
	ErrNotFollowed      = errors.New("not followed")
	ErrFollowerNotFound = errors.New("follower account not found")
	ErrVloggerNotFound  = errors.New("vlogger account not found")
)

// Social 关注关系实体（粉丝 -> 博主）。
type Social struct {
	ID         uint
	FollowerID uint
	VloggerID  uint
}

// Validate 校验关注关系的基础业务规则。
func (s *Social) Validate() error {
	if s == nil || s.FollowerID == 0 || s.VloggerID == 0 {
		return ErrInvalidRelation
	}
	if s.FollowerID == s.VloggerID {
		return ErrCannotFollowSelf
	}
	return nil
}

// AccountBrief 账号摘要，只包含社交列表需要展示的字段。
type AccountBrief struct {
	ID       uint
	Username string
}
