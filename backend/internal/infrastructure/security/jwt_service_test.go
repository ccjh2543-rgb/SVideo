package security

import (
	"errors"
	"testing"
	"time"

	domainaccount "feedsystem_video_go/internal/domain/account"
)

func TestJWTServiceIssueAndParse(t *testing.T) {
	service := NewJWTService([]byte("test-secret"), time.Minute, time.Hour)

	pair, err := service.Issue(42, "alice", "")
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}
	if pair.RefreshJTI == "" || pair.FamilyID == "" {
		t.Fatal("expected refresh jti and family id")
	}

	accessClaims, err := service.ParseAccess(pair.AccessToken)
	if err != nil {
		t.Fatalf("parse access token: %v", err)
	}
	if accessClaims.AccountID != 42 || accessClaims.Username != "alice" {
		t.Fatalf("unexpected access claims: %+v", accessClaims)
	}
	if accessClaims.TokenType != domainaccount.TokenTypeAccess {
		t.Fatalf("unexpected access token type: %s", accessClaims.TokenType)
	}

	refreshClaims, err := service.ParseRefresh(pair.RefreshToken)
	if err != nil {
		t.Fatalf("parse refresh token: %v", err)
	}
	if refreshClaims.JTI != pair.RefreshJTI || refreshClaims.FamilyID != pair.FamilyID {
		t.Fatalf("unexpected refresh claims: %+v", refreshClaims)
	}
}

func TestJWTServiceRejectsWrongTokenType(t *testing.T) {
	service := NewJWTService([]byte("test-secret"), time.Minute, time.Hour)
	pair, err := service.Issue(1, "alice", "")
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}

	if _, err := service.ParseRefresh(pair.AccessToken); !errors.Is(err, domainaccount.ErrTokenInvalid) {
		t.Fatalf("expected access token to be rejected as refresh, got %v", err)
	}
	if _, err := service.ParseAccess(pair.RefreshToken); !errors.Is(err, domainaccount.ErrTokenInvalid) {
		t.Fatalf("expected refresh token to be rejected as access, got %v", err)
	}
}

func TestJWTServiceRejectsExpiredAccessToken(t *testing.T) {
	service := NewJWTService([]byte("test-secret"), -time.Minute, time.Hour)
	pair, err := service.Issue(1, "alice", "")
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}
	if _, err := service.ParseAccess(pair.AccessToken); !errors.Is(err, domainaccount.ErrTokenInvalid) {
		t.Fatalf("expected expired token error, got %v", err)
	}
}

func TestJWTServiceRejectsTamperedToken(t *testing.T) {
	service := NewJWTService([]byte("test-secret"), time.Minute, time.Hour)
	pair, err := service.Issue(1, "alice", "")
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}
	tampered := pair.AccessToken + "x"
	if _, err := service.ParseAccess(tampered); !errors.Is(err, domainaccount.ErrTokenInvalid) {
		t.Fatalf("expected tampered token error, got %v", err)
	}
}

func TestNewJWTServiceFromEnvRequiresSecret(t *testing.T) {
	t.Setenv("JWT_SECRET", "")
	if _, err := NewJWTServiceFromEnv(); err == nil {
		t.Fatal("expected error when JWT_SECRET is empty")
	}

	t.Setenv("JWT_SECRET", "test-secret")
	if _, err := NewJWTServiceFromEnv(); err != nil {
		t.Fatalf("expected service from env, got %v", err)
	}
}
