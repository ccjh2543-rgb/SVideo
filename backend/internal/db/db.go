package db

import (
	"feedsystem_video_go/internal/config"
	accountmysql "feedsystem_video_go/internal/infrastructure/persistence/mysql"
	socialmysql "feedsystem_video_go/internal/infrastructure/persistence/mysql/social"
	videomysql "feedsystem_video_go/internal/infrastructure/persistence/mysql/video"
	"fmt"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func NewDB(dbcfg config.DatabaseConfig) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		dbcfg.User, dbcfg.Password, dbcfg.Host, dbcfg.Port, dbcfg.DBName)

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	return db, nil
}

func AutoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(
		&accountmysql.AccountModel{},
		&accountmysql.RefreshTokenModel{},
		&socialmysql.StatsModel{},
		&accountmysql.ProcessedEventModel{},
		&videomysql.VideoModel{},
		&videomysql.LikeModel{},
		&videomysql.CommentModel{},
		&socialmysql.SocialModel{},
		&videomysql.OutboxModel{},
	)
}

func CloseDB(db *gorm.DB) error {
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}
