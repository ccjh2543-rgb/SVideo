package account

// registerRequest 注册请求。
type registerRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// loginRequest 登录请求。
type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// refreshRequest 刷新 Token 请求。
type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

// logoutRequest 登出请求。
type logoutRequest struct {
	RefreshToken string `json:"refresh_token"`
}

// changePasswordRequest 修改密码请求。
// 账号身份从 Access Token 中获取，不再通过用户名参数传递。
type changePasswordRequest struct {
	OldPassword string `json:"old_password"`
	NewPassword string `json:"new_password"`
}

// renameRequest 修改用户名请求。
type renameRequest struct {
	NewUsername string `json:"new_username"`
}

// findRequest 查询账号请求。
type findRequest struct {
	ID       uint   `json:"id"`
	Username string `json:"username"`
}

// tokenResponse 双 Token 响应。
// token 字段保留给旧版前端和压测脚本使用，等同于 access_token。
type tokenResponse struct {
	Token            string `json:"token"`
	AccessToken      string `json:"access_token"`
	RefreshToken     string `json:"refresh_token"`
	TokenType        string `json:"token_type"`
	ExpiresIn        int64  `json:"expires_in"`
	RefreshExpiresIn int64  `json:"refresh_expires_in"`
}

// accountResponse 账号信息响应，避免暴露密码和 Token。
type accountResponse struct {
	ID       uint   `json:"id"`
	Username string `json:"username"`
}
