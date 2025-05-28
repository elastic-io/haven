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

/*
package api

import (
	"fmt"
	"net/http"
	"time"

	"github.com/elastic-io/haven/internal/config"
	"github.com/elastic-io/haven/internal/log"
	"github.com/gorilla/mux"
)

type API interface {
	Init(*config.Config)
	RegisterRoutes(*mux.Router)
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
	router *mux.Router
	apis   []API
}

func New(c *config.Config) *Server {
	s := &Server{
		config: c,
		router: mux.NewRouter(),
	}
	s.router.Use(loggingMiddleware)
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
		api.RegisterRoutes(s.router)
		s.apis = append(s.apis, api)
	}
    return nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

func (s *Server) Serve() error {
	if s.config.CertFile != "" && s.config.KeyFile != "" {
		log.Logger.Info("Using HTTPS with certificate: ", s.config.CertFile, "and key: ", s.config.KeyFile)
		if err := http.ListenAndServeTLS(s.config.Endpoint, s.config.CertFile, s.config.KeyFile, s); err != nil {
			log.Logger.Fatalf("HTTPS server failed:", err)
		}
		return nil
	}

	log.Logger.Warn("WARNING: Using insecure HTTP mode")
	if err := http.ListenAndServe(s.config.Endpoint, s); err != nil {
		log.Logger.Fatalf("HTTP server failed: ", err)
	}
    return nil
}

func (s *Server) Done() error {
    return nil
}

func BasicAuthMiddleware(next http.Handler, username, password string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()

		if !ok || user != username || pass != password {
			w.Header().Set("WWW-Authenticate", `Basic realm="Docker Registry"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		log.Logger.Info(r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
		log.Logger.Info(r.Method, r.URL.Path, " completed in ", time.Since(start))
	})
}
*/