package account

import (
	"context"
	"errors"
	"testing"
	"time"

	domainaccount "feedsystem_video_go/internal/domain/account"
	"feedsystem_video_go/internal/infrastructure/security"
)

// fakeAccountRepository 内存账号仓储，用于应用层单元测试。
type fakeAccountRepository struct {
	nextID   uint
	accounts map[uint]*domainaccount.Account
	byName   map[string]*domainaccount.Account
}

func newFakeAccountRepository() *fakeAccountRepository {
	return &fakeAccountRepository{
		nextID:   1,
		accounts: make(map[uint]*domainaccount.Account),
		byName:   make(map[string]*domainaccount.Account),
	}
}

func (r *fakeAccountRepository) Create(_ context.Context, account *domainaccount.Account) error {
	if _, exists := r.byName[account.Username]; exists {
		return domainaccount.ErrUsernameTaken
	}
	account.ID = r.nextID
	r.nextID++
	copied := *account
	r.accounts[account.ID] = &copied
	r.byName[account.Username] = &copied
	return nil
}

func (r *fakeAccountRepository) FindByID(_ context.Context, id uint) (*domainaccount.Account, error) {
	account, exists := r.accounts[id]
	if !exists {
		return nil, domainaccount.ErrAccountNotFound
	}
	copied := *account
	return &copied, nil
}

func (r *fakeAccountRepository) FindByUsername(_ context.Context, username string) (*domainaccount.Account, error) {
	account, exists := r.byName[username]
	if !exists {
		return nil, domainaccount.ErrAccountNotFound
	}
	copied := *account
	return &copied, nil
}

func (r *fakeAccountRepository) UpdateUsername(_ context.Context, id uint, username string) error {
	account, exists := r.accounts[id]
	if !exists {
		return domainaccount.ErrAccountNotFound
	}
	if existing, exists := r.byName[username]; exists && existing.ID != id {
		return domainaccount.ErrUsernameTaken
	}
	delete(r.byName, account.Username)
	account.Username = username
	r.byName[username] = account
	return nil
}

func (r *fakeAccountRepository) UpdatePassword(_ context.Context, id uint, passwordHash string) error {
	account, exists := r.accounts[id]
	if !exists {
		return domainaccount.ErrAccountNotFound
	}
	account.PasswordHash = passwordHash
	return nil
}

// fakeRefreshTokenRepository 内存刷新令牌仓储。
type fakeRefreshTokenRepository struct {
	records map[string]*domainaccount.RefreshToken
}

func newFakeRefreshTokenRepository() *fakeRefreshTokenRepository {
	return &fakeRefreshTokenRepository{records: make(map[string]*domainaccount.RefreshToken)}
}

func (r *fakeRefreshTokenRepository) Create(_ context.Context, token *domainaccount.RefreshToken) error {
	copied := *token
	r.records[token.JTI] = &copied
	return nil
}

func (r *fakeRefreshTokenRepository) FindByJTI(_ context.Context, jti string) (*domainaccount.RefreshToken, error) {
	record, exists := r.records[jti]
	if !exists {
		return nil, domainaccount.ErrTokenInvalid
	}
	copied := *record
	return &copied, nil
}

func (r *fakeRefreshTokenRepository) RevokeIfActive(_ context.Context, jti string) (bool, error) {
	record, exists := r.records[jti]
	if !exists || record.RevokedAt != nil {
		return false, nil
	}
	now := time.Now()
	record.RevokedAt = &now
	return true, nil
}

func (r *fakeRefreshTokenRepository) RevokeFamily(_ context.Context, familyID string) error {
	now := time.Now()
	for _, record := range r.records {
		if record.FamilyID == familyID && record.RevokedAt == nil {
			record.RevokedAt = &now
		}
	}
	return nil
}

func (r *fakeRefreshTokenRepository) RevokeAllByAccount(_ context.Context, accountID uint) error {
	now := time.Now()
	for _, record := range r.records {
		if record.AccountID == accountID && record.RevokedAt == nil {
			record.RevokedAt = &now
		}
	}
	return nil
}

// fakePasswordHasher 可预测的密码哈希器，避免单元测试使用 bcrypt 拖慢速度。
type fakePasswordHasher struct{}

func (fakePasswordHasher) Hash(password string) (string, error) {
	return "hash:" + password, nil
}

func (fakePasswordHasher) Compare(hash string, password string) error {
	if hash != "hash:"+password {
		return errors.New("password mismatch")
	}
	return nil
}

type testFixture struct {
	service       *Service
	accounts      *fakeAccountRepository
	refreshTokens *fakeRefreshTokenRepository
}

func newTestFixture(t *testing.T) *testFixture {
	t.Helper()
	t.Setenv("JWT_SECRET", "unit-test-secret")
	tokens := security.NewJWTService([]byte("unit-test-secret"), time.Minute, time.Hour)
	accounts := newFakeAccountRepository()
	refreshTokens := newFakeRefreshTokenRepository()
	return &testFixture{
		service:       NewService(accounts, refreshTokens, tokens, fakePasswordHasher{}),
		accounts:      accounts,
		refreshTokens: refreshTokens,
	}
}

func TestRegisterAndLogin(t *testing.T) {
	f := newTestFixture(t)
	ctx := context.Background()

	if err := f.service.Register(ctx, "alice", "password"); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := f.service.Register(ctx, "alice", "password"); !errors.Is(err, domainaccount.ErrUsernameTaken) {
		t.Fatalf("expected duplicate username error, got %v", err)
	}
	if _, err := f.service.Login(ctx, "alice", "wrong"); !errors.Is(err, domainaccount.ErrInvalidCredentials) {
		t.Fatalf("expected invalid credentials, got %v", err)
	}

	pair, err := f.service.Login(ctx, "alice", "password")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if pair.AccessToken == "" || pair.RefreshToken == "" {
		t.Fatal("expected both tokens")
	}
	if _, exists := f.refreshTokens.records[pair.RefreshJTI]; !exists {
		t.Fatal("expected refresh token to be persisted")
	}
}

func TestRefreshRotatesAndDetectsReuse(t *testing.T) {
	f := newTestFixture(t)
	ctx := context.Background()

	if err := f.service.Register(ctx, "alice", "password"); err != nil {
		t.Fatalf("register: %v", err)
	}
	first, err := f.service.Login(ctx, "alice", "password")
	if err != nil {
		t.Fatalf("login: %v", err)
	}

	second, err := f.service.Refresh(ctx, first.RefreshToken)
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if second.RefreshJTI == first.RefreshJTI {
		t.Fatal("expected refresh token rotation")
	}
	if second.FamilyID != first.FamilyID {
		t.Fatal("expected same token family")
	}

	// 旧 Refresh Token 再次使用，应判定为重放并撤销整个 family。
	if _, err := f.service.Refresh(ctx, first.RefreshToken); !errors.Is(err, domainaccount.ErrTokenRevoked) {
		t.Fatalf("expected token revoked, got %v", err)
	}
	if record := f.refreshTokens.records[second.RefreshJTI]; record == nil || record.RevokedAt == nil {
		t.Fatal("expected family to be revoked after replay")
	}
}

func TestChangePasswordRevokesAllTokens(t *testing.T) {
	f := newTestFixture(t)
	ctx := context.Background()

	if err := f.service.Register(ctx, "alice", "old-password"); err != nil {
		t.Fatalf("register: %v", err)
	}
	account, err := f.service.FindByUsername(ctx, "alice")
	if err != nil {
		t.Fatalf("find account: %v", err)
	}
	pair, err := f.service.Login(ctx, "alice", "old-password")
	if err != nil {
		t.Fatalf("login: %v", err)
	}

	if err := f.service.ChangePassword(ctx, account.ID, "old-password", "new-password"); err != nil {
		t.Fatalf("change password: %v", err)
	}
	if _, err := f.service.Login(ctx, "alice", "old-password"); !errors.Is(err, domainaccount.ErrInvalidCredentials) {
		t.Fatalf("expected old password to fail, got %v", err)
	}
	if _, err := f.service.Login(ctx, "alice", "new-password"); err != nil {
		t.Fatalf("expected new password to work, got %v", err)
	}
	if record := f.refreshTokens.records[pair.RefreshJTI]; record == nil || record.RevokedAt == nil {
		t.Fatal("expected old refresh token to be revoked")
	}
}

func TestLogoutRevokesFamily(t *testing.T) {
	f := newTestFixture(t)
	ctx := context.Background()

	if err := f.service.Register(ctx, "alice", "password"); err != nil {
		t.Fatalf("register: %v", err)
	}
	account, err := f.service.FindByUsername(ctx, "alice")
	if err != nil {
		t.Fatalf("find account: %v", err)
	}
	pair, err := f.service.Login(ctx, "alice", "password")
	if err != nil {
		t.Fatalf("login: %v", err)
	}

	if err := f.service.Logout(ctx, account.ID, pair.RefreshToken); err != nil {
		t.Fatalf("logout: %v", err)
	}
	if record := f.refreshTokens.records[pair.RefreshJTI]; record == nil || record.RevokedAt == nil {
		t.Fatal("expected refresh token to be revoked")
	}
	if _, err := f.service.Refresh(ctx, pair.RefreshToken); !errors.Is(err, domainaccount.ErrTokenRevoked) {
		t.Fatalf("expected refresh after logout to fail, got %v", err)
	}
}

func TestRenameIssuesNewTokens(t *testing.T) {
	f := newTestFixture(t)
	ctx := context.Background()

	if err := f.service.Register(ctx, "alice", "password"); err != nil {
		t.Fatalf("register: %v", err)
	}
	account, err := f.service.FindByUsername(ctx, "alice")
	if err != nil {
		t.Fatalf("find account: %v", err)
	}
	oldPair, err := f.service.Login(ctx, "alice", "password")
	if err != nil {
		t.Fatalf("login: %v", err)
	}

	newPair, err := f.service.Rename(ctx, account.ID, "alice-new")
	if err != nil {
		t.Fatalf("rename: %v", err)
	}
	if newPair.RefreshJTI == oldPair.RefreshJTI {
		t.Fatal("expected new refresh token after rename")
	}
	if record := f.refreshTokens.records[oldPair.RefreshJTI]; record == nil || record.RevokedAt == nil {
		t.Fatal("expected old token to be revoked after rename")
	}
	updated, err := f.service.FindByID(ctx, account.ID)
	if err != nil {
		t.Fatalf("find renamed account: %v", err)
	}
	if updated.Username != "alice-new" {
		t.Fatalf("expected renamed username, got %s", updated.Username)
	}
}
