package video

import (
	"errors"
	"testing"
)

func TestVideoValidateForPublish(t *testing.T) {
	video := &Video{
		Title:    "  demo  ",
		PlayURL:  " http://example.com/a.mp4 ",
		CoverURL: " http://example.com/a.jpg ",
	}
	if err := video.ValidateForPublish(); err != nil {
		t.Fatalf("validate video: %v", err)
	}
	if video.Title != "demo" {
		t.Fatalf("expected trimmed title, got %q", video.Title)
	}
	if video.PlayURL != "http://example.com/a.mp4" {
		t.Fatalf("expected trimmed play url, got %q", video.PlayURL)
	}

	if err := (&Video{}).ValidateForPublish(); !errors.Is(err, ErrTitleRequired) {
		t.Fatalf("expected title required, got %v", err)
	}
	if err := (&Video{Title: "demo"}).ValidateForPublish(); !errors.Is(err, ErrPlayURLRequired) {
		t.Fatalf("expected play url required, got %v", err)
	}
	if err := (&Video{Title: "demo", PlayURL: "x"}).ValidateForPublish(); !errors.Is(err, ErrCoverURLRequired) {
		t.Fatalf("expected cover url required, got %v", err)
	}
}

func TestCommentValidate(t *testing.T) {
	comment := &Comment{VideoID: 1, AuthorID: 2, Content: "  hello  "}
	if err := comment.Validate(); err != nil {
		t.Fatalf("validate comment: %v", err)
	}
	if comment.Content != "hello" {
		t.Fatalf("expected trimmed content, got %q", comment.Content)
	}
	if err := (&Comment{}).Validate(); !errors.Is(err, ErrCommentRequired) {
		t.Fatalf("expected comment required, got %v", err)
	}
	if err := (&Comment{VideoID: 1, AuthorID: 2}).Validate(); !errors.Is(err, ErrCommentContentRequired) {
		t.Fatalf("expected content required, got %v", err)
	}
}
