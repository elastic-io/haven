package api

import (
	"fmt"
	"time"

	"github.com/elastic-io/haven/internal/config"
	"github.com/elastic-io/haven/internal/log"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/basicauth"
)

type API interface {
	Init(*config.Config)
	RegisterRoutes(*fiber.App)
}

var apis = map[string]API{}

func APIRegister(name string, api API) {
	if _, ok := apis[name]; ok {
		panic(fmt.Errorf("API %s already registered", name))
	}
	apis[name] = api
}

type Server struct {
	config *config.Config
	router *fiber.App
	apis   []API
}

func New(c *config.Config) *Server {
	s := &Server{
		config: c,
		router: fiber.New(fiber.Config{
			DisableStartupMessage: true,
			BodyLimit:             c.BodyLimit,
			StrictRouting:         true,
			CaseSensitive:         true,
		}),
	}

	// 添加日志中间件
	s.router.Use(loggingMiddleware())

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
