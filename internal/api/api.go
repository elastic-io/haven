package api

import (
	"fmt"
	"net"
	"runtime/debug"
	"sync"
	"time"

	"github.com/elastic-io/haven/internal/config"
	"github.com/elastic-io/haven/internal/log"
	"github.com/elastic-io/haven/internal/types"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/basicauth"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/cors"

	"github.com/gofiber/fiber/v2/middleware/limiter"
	"github.com/gofiber/fiber/v2/middleware/monitor"
	frecover "github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/valyala/fasthttp"
)

type API interface {
	Init(*config.Config)
	RegisterRoutes(*fiber.App)
}

var apis = map[string]API{}
var apisMu sync.RWMutex

func APIRegister(name string, api API) {
	apisMu.Lock()
	defer apisMu.Unlock()
	if _, ok := apis[name]; ok {
		log.Logger.Fatalf("API %s already registered", name)
	}
	apis[name] = api
}

type Server struct {
	config *config.Config
	router *fiber.App
	apis   []API
	server *fasthttp.Server
}

func New(c *config.Config) *Server {
	app := fiber.New(fiber.Config{
		BodyLimit:                    c.BodyLimit,
		DisableStartupMessage:        true,
		StrictRouting:                true,
		CaseSensitive:                true,
		StreamRequestBody:            true,
		ReadTimeout:                  time.Duration(c.ReadTimeout) * time.Second,
		WriteTimeout:                 time.Duration(c.WriteTimeout) * time.Second,
		IdleTimeout:                  time.Duration(c.IdleTimeout) * time.Second,
		ReduceMemoryUsage:            true,
		DisablePreParseMultipartForm: true,
		ReadBufferSize:               32 * types.KB,
		WriteBufferSize:              32 * types.KB,
		DisableKeepalive:             false,
		Concurrency:                  256 * 1024,
		ServerHeader:                 "Haven Server",
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError
			if e, ok := err.(*fiber.Error); ok {
				code = e.Code
			}
			log.Logger.Errorf("HTTP Error: %v Path: %s IP: %s", err, c.Path(), c.IP())
			return c.Status(code).JSON(fiber.Map{
				"error": err.Error(),
			})
		},
	})

	// 添加中间件
	app.Use(frecover.New(frecover.Config{
		EnableStackTrace: true,
		StackTraceHandler: func(c *fiber.Ctx, e interface{}) {
			log.Logger.Errorf("Recovered from panic in HTTP handler: %v\n%s", e, debug.Stack())
			c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Internal Server Error",
			})
		},
	}))
	app.Use(compress.New())
	app.Use(cors.New())
	app.Use(limiter.New(limiter.Config{
		Max:        types.KB,
		Expiration: 5 * time.Minute,
	}))
	app.Use(loggingMiddleware())

	// 添加监控端点
	app.Get("/metrics", monitor.New())

	s := &Server{
		config: c,
		router: app,
	}

	// 创建自定义的 fasthttp 服务器
	s.server = &fasthttp.Server{
		Handler:               s.router.Handler(),
		Name:                  "Haven Custom Server",
		ReadTimeout:           time.Duration(c.ReadTimeout) * time.Second,
		WriteTimeout:          time.Duration(c.WriteTimeout) * time.Second,
		IdleTimeout:           time.Duration(c.IdleTimeout) * time.Second,
		MaxRequestBodySize:    c.BodyLimit,
		NoDefaultServerHeader: true,
		MaxConnsPerIP:         100,
		MaxRequestsPerConn:    1000,
		ReduceMemoryUsage:     true,
		TCPKeepalive:          true,
		TCPKeepalivePeriod:    time.Minute,
		DisableKeepalive:      false,
		// 自定义连接处理，添加panic恢复
		ConnState: func(conn net.Conn, state fasthttp.ConnState) {
			defer func() {
				if r := recover(); r != nil {
					log.Logger.Errorf("Recovered from connection panic: %v\n%s", r, debug.Stack())
					conn.Close()
				}
			}()
		},
	}

	return s
}

func (s *Server) Init() error {
	if len(apis) == 0 {
		return fmt.Errorf("no APIs registered")
	}

	for _, mod := range s.config.Modules {
		if api, ok := apis[mod]; ok {
			s.apis = append(s.apis, api)
		}
	}

	for _, api := range s.apis {
		api.Init(s.config)
		api.RegisterRoutes(s.router)
	}

	return nil
}

func (s *Server) Serve() error {
	log.Logger.Info("Starting server on ", s.config.Endpoint)

	if s.config.CertFile != "" && s.config.KeyFile != "" {
		log.Logger.Info("Using HTTPS with certificate: ", s.config.CertFile, " and key: ", s.config.KeyFile)
		return s.router.ListenTLS(s.config.Endpoint, s.config.CertFile, s.config.KeyFile)
	}

	log.Logger.Warn("WARNING: Using insecure HTTP mode")
	return s.router.Listen(s.config.Endpoint)
}

func (s *Server) Done() error {
	if s.router != nil {
		return s.router.Shutdown()
	}
	if s.server != nil {
		return s.server.Shutdown()
	}
	return nil
}

func BasicAuthMiddleware(username, password string) fiber.Handler {
	return basicauth.New(basicauth.Config{
		Users: map[string]string{
			username: password,
		},
		Realm: "Docker Registry",
		Unauthorized: func(c *fiber.Ctx) error {
			return c.Status(fiber.StatusUnauthorized).SendString("Unauthorized")
		},
	})
}

func loggingMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		start := time.Now()
		log.Logger.Info(c.Method(), " ", c.Path())

		err := c.Next()

		log.Logger.Info(c.Method(), " ", c.Path(), " completed in ", time.Since(start))
		return err
	}
}