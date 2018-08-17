package log

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var art = `
 ________  ________  ________   ________  ________  ________
|\   __  \|\   __  \|\   ___  \|\   ___ \|\   __  \|\   ____\ 
\ \  \|\  \ \  \|\  \ \  \\ \  \ \  \_|\ \ \  \|\  \ \  \___|_
 \ \   ____\ \   __  \ \  \\ \  \ \  \ \\ \ \  \\\  \ \_____  \ 
  \ \  \___|\ \  \ \  \ \  \\ \  \ \  \_\\ \ \  \\\  \|____|\  \ 
   \ \__\    \ \__\ \__\ \__\\ \__\ \_______\ \_______\____\_\  \ 
    \|__|     \|__|\|__|\|__| \|__|\|_______|\|_______|\_________\
                                                      \|_________|
`

func shortTime(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format("01/02 15:04:05"))
}

var (
	// LoggerInstance is the global logger for pandos
	LoggerInstance *zap.Logger

	once sync.Once
)

// Logger is the global logger for logging
func Logger() *zap.Logger {
	once.Do(func() {
		cfg := zap.NewDevelopmentConfig()
		cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		cfg.EncoderConfig.EncodeTime = shortTime

		l, _ := cfg.Build()
		LoggerInstance = l
	})

	return LoggerInstance
}

// PrintStartupInfo prints out useful node information on startup
func PrintStartupInfo(id, verbose, storage, advertiseHost, port, peer string) {
	fmt.Println(art)
	fmt.Printf("id: \t\t%s\n", id)
	fmt.Printf("log level: \t%s\n", verbose)
	fmt.Printf("storageDir: \t%s\n", storage)
	fmt.Printf("hostname: \t%s:%s\n", advertiseHost, port)
	if peer != "" {
		fmt.Printf("peer: \t\t%s\n\n", peer)
	}
	fmt.Print("\n")
}
