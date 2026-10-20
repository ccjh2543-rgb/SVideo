package video

// PublishVideoRequest 发布视频请求。
type PublishVideoRequest struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	PlayURL     string `json:"play_url"`
	CoverURL    string `json:"cover_url"`
}

// ListByAuthorIDRequest 按作者查询视频请求。
type ListByAuthorIDRequest struct {
	AuthorID uint `json:"author_id"`
}

// GetDetailRequest 视频详情请求。
type GetDetailRequest struct {
	ID uint `json:"id"`
}

// LikeRequest 点赞/取消点赞/点赞状态请求。
type LikeRequest struct {
	VideoID uint `json:"video_id"`
}

// PublishCommentRequest 发布评论请求。
type PublishCommentRequest struct {
	VideoID uint   `json:"video_id"`
	Content string `json:"content"`
}

// DeleteCommentRequest 删除评论请求。
type DeleteCommentRequest struct {
	CommentID uint `json:"comment_id"`
}

// GetAllCommentsRequest 查询评论请求。
type GetAllCommentsRequest struct {
	VideoID uint `json:"video_id"`
}
