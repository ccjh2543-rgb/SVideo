package security

import "golang.org/x/crypto/bcrypt"

// BcryptHasher 使用 bcrypt 进行密码哈希与校验。
type BcryptHasher struct {
	cost int
}

// NewBcryptHasher 创建默认成本的密码哈希器。
func NewBcryptHasher() *BcryptHasher {
	return &BcryptHasher{cost: bcrypt.DefaultCost}
}

// Hash 生成密码哈希。
func (h *BcryptHasher) Hash(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), h.cost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

// Compare 校验原始密码与哈希是否匹配。
func (h *BcryptHasher) Compare(hash string, password string) error {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
}
