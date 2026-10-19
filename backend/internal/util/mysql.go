package util

import (
	"errors"

	"github.com/go-sql-driver/mysql"
)

// IsDuplicateEntry 判断给定的错误是否为 MySQL 1062 重复键冲突错误。
// 常用于幂等处理场景：插入时遇到唯一键冲突视为成功，无需重试。
func IsDuplicateEntry(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1062
}