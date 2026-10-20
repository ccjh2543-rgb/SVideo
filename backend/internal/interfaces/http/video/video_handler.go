package video

import (
	"fmt"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	appvideo "feedsystem_video_go/internal/application/video"
	domainvideo "feedsystem_video_go/internal/domain/video"
	"feedsystem_video_go/internal/interfaces/http/middleware/jwt"
	"feedsystem_video_go/internal/util"

	"github.com/gin-gonic/gin"
)

// VideoHandler 视频 HTTP 处理器。
type VideoHandler struct {
	service *appvideo.VideoService
}

// NewVideoHandler 创建视频处理器。
func NewVideoHandler(service *appvideo.VideoService) *VideoHandler {
	return &VideoHandler{service: service}
}

// PublishVideo 发布视频。
func (h *VideoHandler) PublishVideo(c *gin.Context) {
	var req PublishVideoRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	authorID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	username, err := jwt.GetUsername(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	video := &domainvideo.Video{
		AuthorID:    authorID,
		Username:    username,
		Title:       req.Title,
		Description: req.Description,
		PlayURL:     req.PlayURL,
		CoverURL:    req.CoverURL,
		CreateTime:  time.Now(),
	}
	if err := h.service.Publish(c.Request.Context(), video); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, video)
}

// ListByAuthorID 查询作者视频。
func (h *VideoHandler) ListByAuthorID(c *gin.Context) {
	var req ListByAuthorIDRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	videos, err := h.service.ListByAuthorID(c.Request.Context(), req.AuthorID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, videos)
}

// GetDetail 查询视频详情。
func (h *VideoHandler) GetDetail(c *gin.Context) {
	var req GetDetailRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	video, err := h.service.GetDetail(c.Request.Context(), req.ID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, video)
}

// UploadVideo 上传视频文件。
func (h *VideoHandler) UploadVideo(c *gin.Context) {
	authorID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	f, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing file"})
		return
	}
	const maxSize = 200 << 20
	if f.Size <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "file is empty"})
		return
	}
	if f.Size > maxSize {
		c.JSON(http.StatusBadRequest, gin.H{"error": "file too large, max 200MB for video"})
		return
	}
	ext := strings.ToLower(filepath.Ext(f.Filename))
	if ext != ".mp4" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "only .mp4 format is allowed"})
		return
	}
	urlPath, err := saveUpload(c, f, "videos", authorID, ext)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"url":      buildAbsoluteURL(c, urlPath),
		"play_url": buildAbsoluteURL(c, urlPath),
	})
}

// UploadCover 上传封面文件。
func (h *VideoHandler) UploadCover(c *gin.Context) {
	authorID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	f, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing file"})
		return
	}
	const maxSize = 10 << 20
	if f.Size <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "file is empty"})
		return
	}
	if f.Size > maxSize {
		c.JSON(http.StatusBadRequest, gin.H{"error": "file too large, max 10MB for cover"})
		return
	}
	ext := strings.ToLower(filepath.Ext(f.Filename))
	switch ext {
	case ".jpg", ".jpeg", ".png", ".webp":
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": "only .jpg/.jpeg/.png/.webp formats are allowed"})
		return
	}
	urlPath, err := saveUpload(c, f, "covers", authorID, ext)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"url":       buildAbsoluteURL(c, urlPath),
		"cover_url": buildAbsoluteURL(c, urlPath),
	})
}

func saveUpload(c *gin.Context, fileHeader *multipart.FileHeader, category string, authorID uint, ext string) (string, error) {
	date := time.Now().Format("20060102")
	relDir := filepath.Join(category, fmt.Sprintf("%d", authorID), date)
	root := filepath.Join(".run", "uploads")
	absDir := filepath.Join(root, relDir)
	if err := os.MkdirAll(absDir, 0o755); err != nil {
		return "", err
	}
	hex, err := util.RandHex(16)
	if err != nil {
		return "", err
	}
	filename := hex + ext
	if err := c.SaveUploadedFile(fileHeader, filepath.Join(absDir, filename)); err != nil {
		return "", err
	}
	return path.Join("/static", category, fmt.Sprintf("%d", authorID), date, filename), nil
}

func buildAbsoluteURL(c *gin.Context, p string) string {
	scheme := "http"
	if c.Request.TLS != nil {
		scheme = "https"
	}
	if xf := c.GetHeader("X-Forwarded-Proto"); xf != "" {
		scheme = xf
	}
	return fmt.Sprintf("%s://%s%s", scheme, c.Request.Host, p)
}
