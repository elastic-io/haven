package api

import (
	"fmt"
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
	Init(*config.SourceConfig)
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
	config *config.SourceConfig
	router *fiber.App
	apis   []API
	server *fasthttp.Server
}

func New(c *config.SourceConfig) *Server {
	app := fiber.New(fiber.Config{
		BodyLimit:                    c.BodySize,
		DisableStartupMessage:        true,
		StrictRouting:                true,
		CaseSensitive:                true,
		StreamRequestBody:            true,
		ReadTimeout:                  time.Duration(c.ReadTimeout) * time.Second,
		WriteTimeout:                 time.Duration(c.WriteTimeout) * time.Second,
		IdleTimeout:                  time.Duration(c.IdleTimeout) * time.Second,
		ReduceMemoryUsage:            false,
		DisablePreParseMultipartForm: true,
		ReadBufferSize:               32 * types.KB, // 减小缓冲区
		WriteBufferSize:              32 * types.KB,
		DisableKeepalive:             false,
		Concurrency:                  256 * types.KB,
		ServerHeader:                 "Haven Fiber Server",
		Network:                      "tcp",
		EnableTrustedProxyCheck:      false,
		TrustedProxies:               []string{},
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

	app.Get("/metrics", monitor.New())

	return &Server{config: c, router: app}
}

// 修改Serve方法，使用自定义的fasthttp服务器
func (s *Server) Serve() error {
	s.addGlobalErrorRecovery()

	log.Logger.Info("Starting server on ", s.config.Endpoint)

	// 创建自定义的fasthttp服务器
	server := &fasthttp.Server{
		Handler:               s.router.Handler(),
		Name:                  "Haven Fasthttp Server",
		ReadTimeout:           time.Duration(s.config.ReadTimeout) * time.Second,
		WriteTimeout:          time.Duration(s.config.WriteTimeout) * time.Second,
		IdleTimeout:           time.Duration(s.config.IdleTimeout) * time.Second,
		MaxRequestBodySize:    s.config.BodySize,
		NoDefaultServerHeader: true,
		MaxConnsPerIP:         50,   // 降低连接数限制
		MaxRequestsPerConn:    500,  // 降低请求数限制
		ReduceMemoryUsage:     true, // 启用内存优化
		TCPKeepalive:          true,
		TCPKeepalivePeriod:    30 * time.Second, // 缩短keepalive时间
		DisableKeepalive:      false,
		ReadBufferSize:        16 * types.KB, // 进一步减小缓冲区
		WriteBufferSize:       16 * types.KB,
		// 添加更严格的限制
		MaxKeepaliveDuration: 60 * time.Second,
		// 自定义错误处理
		ErrorHandler: func(ctx *fasthttp.RequestCtx, err error) {
			log.Logger.Errorf("FastHTTP Error: %v, RemoteAddr: %s", err, ctx.RemoteAddr())
			ctx.Error("Bad Request", fasthttp.StatusBadRequest)
		},
	}

	// 包装handler以捕获panic
	originalHandler := server.Handler
	server.Handler = func(ctx *fasthttp.RequestCtx) {
		defer func() {
			if r := recover(); r != nil {
				log.Logger.Errorf("Recovered from fasthttp panic: %v\n%s", r, debug.Stack())
				if !ctx.Response.ConnectionClose() {
					ctx.Error("Internal Server Error", fasthttp.StatusInternalServerError)
				}
			}
		}()
		originalHandler(ctx)
	}

	s.server = server

	if s.config.CertFile != "" && s.config.KeyFile != "" {
		log.Logger.Info("Using HTTPS with certificate: ", s.config.CertFile, " and key: ", s.config.KeyFile)
		return server.ListenAndServeTLS(s.config.Endpoint, s.config.CertFile, s.config.KeyFile)
	}

	log.Logger.Warn("WARNING: Using insecure HTTP mode")
	return server.ListenAndServe(s.config.Endpoint)
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

func (s *Server) addGlobalErrorRecovery() {
	s.router.Use(func(c *fiber.Ctx) error {
		defer func() {
			if r := recover(); r != nil {
				log.Logger.Errorf("Global panic recovery: %v\n%s", r, debug.Stack())
				c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"error": "Internal Server Error",
				})
			}
		}()
		return c.Next()
	})
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
