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
	app    *fiber.App
	apis   []API
}

func New(c *config.Config) *Server {
	s := &Server{
		config: c,
		app:    fiber.New(fiber.Config{
			DisableStartupMessage: true,
			BodyLimit: 10 * 1024 * 1024 * 1024, // 10GB
			StrictRouting: true,
    		CaseSensitive: true,
		}),
	}
	
	// 添加日志中间件
	s.app.Use(loggingMiddleware())
	
	return s
}

func (s *Server) Init() error {
	if len(apis) == 0 {
		return fmt.Errorf("no APIs registered")
	}
	
	apiMods := map[string]API{}
	for _, mod := range s.config.Modules {
		if api, ok := apis[mod]; ok {
			apiMods[mod] = api
		}
	}
	
	for _, api := range apiMods {
		api.Init(s.config)
		api.RegisterRoutes(s.app)
		s.apis = append(s.apis, api)
	}
	
	return nil
}

func (s *Server) Serve() error {
	log.Logger.Info("Starting server on ", s.config.Endpoint)
	
	if s.config.CertFile != "" && s.config.KeyFile != "" {
		log.Logger.Info("Using HTTPS with certificate: ", s.config.CertFile, " and key: ", s.config.KeyFile)
		return s.app.ListenTLS(s.config.Endpoint, s.config.CertFile, s.config.KeyFile)
	}
	
	log.Logger.Warn("WARNING: Using insecure HTTP mode")
	return s.app.Listen(s.config.Endpoint)
}

func (s *Server) Done() error {
	if s.app != nil {
		return s.app.Shutdown()
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
		log.Logger.Info(c.Method(), c.Path())
		
		err := c.Next()
		
		log.Logger.Info(c.Method(), c.Path(), " completed in ", time.Since(start))
		return err
	}
}