# Rate limit per IP (Gin, chi, echo, Fiber)

Attach a keyed limiter as framework middleware. The default key function is
`KeyByIP()` (X-Forwarded-For → X-Real-IP → RemoteAddr), so a single
`tokenbucket.New(capacity, refillRate)` instance transparently maintains one
bucket per client IP. On deny, each middleware sets the standard
`X-RateLimit-*` / `Retry-After` headers and returns HTTP 429.

Swap `tokenbucket.New` for `gcra.New(limit, burst, window)` or any other
constructor — they all satisfy `ratelimit.Limiter`, so the middleware is
identical.

## Gin

Import: `github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/contrib/ginmw`

```go
package main

import (
	"time"

	"github.com/gin-gonic/gin"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/contrib/ginmw"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

func main() {
	// 100 req burst, refilling at 20/s — per IP.
	lim := tokenbucket.New(100, 20, tokenbucket.WithIdleCleanup(5*time.Minute))
	defer lim.Close()

	r := gin.Default()
	r.Use(ginmw.RateLimit(lim)) // default key = KeyByIP()

	r.GET("/", func(c *gin.Context) { c.String(200, "ok") })
	_ = r.Run(":8080")
}
```

## chi

Import: `github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/contrib/chimw`

`chimw` re-exports the core `ratelimit/middleware` options, so `RateLimit`
returns a standard `func(http.Handler) http.Handler`.

```go
package main

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/contrib/chimw"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

func main() {
	lim := tokenbucket.New(100, 20)
	defer lim.Close()

	r := chi.NewRouter()
	r.Use(chimw.RateLimit(lim)) // default key = KeyByIP()

	r.Get("/", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	_ = http.ListenAndServe(":8080", r)
}
```

## echo

Import: `github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/contrib/echomw`

```go
package main

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/contrib/echomw"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

func main() {
	lim := tokenbucket.New(100, 20)
	defer lim.Close()

	e := echo.New()
	e.Use(echomw.RateLimit(lim)) // default key = KeyByIP()

	e.GET("/", func(c echo.Context) error { return c.String(http.StatusOK, "ok") })
	_ = e.Start(":8080")
}
```

## Fiber

Import: `github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/contrib/fibermw`

```go
package main

import (
	"github.com/gofiber/fiber/v2"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/contrib/fibermw"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

func main() {
	lim := tokenbucket.New(100, 20)
	defer lim.Close()

	app := fiber.New()
	app.Use(fibermw.RateLimit(lim)) // default key = KeyByIP()

	app.Get("/", func(c *fiber.Ctx) error { return c.SendString("ok") })
	_ = app.Listen(":8080")
}
```

## Keying by something other than IP

Every middleware exposes `WithKeyFunc` plus `KeyByIP()`, `KeyByHeader(name)`,
and `KeyByParam(name)` (Gin/chi/echo/Fiber). For example, rate-limit by API key
header instead of IP:

```go
r.Use(ginmw.RateLimit(lim, ginmw.WithKeyFunc(ginmw.KeyByHeader("X-API-Key"))))
```

Or supply a fully custom extractor:

```go
r.Use(ginmw.RateLimit(lim, ginmw.WithKeyFunc(func(c *gin.Context) string {
	return "tenant:" + c.GetHeader("X-Tenant-ID")
})))
```

> **gRPC / Connect:** for Connect RPC use
> `github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/contrib/connectmw` —
> `connectmw.RateLimit(lim, connectmw.WithKeyFunc(connectmw.KeyByPeer()))`
> returns a `connect.Interceptor`. Its key funcs are `KeyByPeer()` and
> `KeyByHeader(name)`.

## See also

- [Per-tenant / per-API-key quotas](per-tenant-quotas.md)
- [Cost / weight-based limiting](cost-weighted-limiting.md)
</content>
