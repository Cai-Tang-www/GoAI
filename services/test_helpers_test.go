package services

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var sqliteTestSequence atomic.Uint64

// openSQLiteTestDB 为每次测试建立独立的 SQLite 内存库，并在测试结束时释放连接。
func openSQLiteTestDB(t *testing.T, suffix ...string) *gorm.DB {
	t.Helper()

	name := strings.NewReplacer("/", "_", " ", "_", ":", "_").Replace(t.Name())
	if len(suffix) > 0 && suffix[0] != "" {
		name += "_" + suffix[0]
	}
	dsn := fmt.Sprintf("file:%s_%d?mode=memory&cache=shared", name, sqliteTestSequence.Add(1))
	database, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}

	sqlDB, err := database.DB()
	if err != nil {
		t.Fatalf("get sqlite connection failed: %v", err)
	}
	t.Cleanup(func() {
		if err := sqlDB.Close(); err != nil {
			t.Errorf("close sqlite connection failed: %v", err)
		}
	})
	return database
}
