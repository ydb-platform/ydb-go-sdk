package balancers

import (
	"encoding/json"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
)

type balancerType string

const (
	typeRoundRobin   = balancerType("round_robin")
	typeRandomChoice = balancerType("random_choice")
	typeSingle       = balancerType("single")
)

type preferType string

const (
	preferLocalDC   = preferType("local_dc")
	preferLocations = preferType("locations")
)

type balancersConfig struct {
	Type      balancerType `json:"type"`
	Prefer    preferType   `json:"prefer,omitempty"`
	Fallback  bool         `json:"fallback,omitempty"`
	Locations []string     `json:"locations,omitempty"`
}

type fromConfigOptionsHolder struct {
	fallbackBalancer balancer.Balancer
	errorHandler     func(error)
}

type fromConfigOption func(h *fromConfigOptionsHolder)

func WithParseErrorFallbackBalancer(b balancer.Balancer) fromConfigOption {
	return func(h *fromConfigOptionsHolder) {
		h.fallbackBalancer = b
	}
}

func WithParseErrorHandler(errorHandler func(error)) fromConfigOption {
	return func(h *fromConfigOptionsHolder) {
		h.errorHandler = errorHandler
	}
}

func FromConfig(config string, opts ...fromConfigOption) (b balancer.Balancer) {
	var (
		h = fromConfigOptionsHolder{
			fallbackBalancer: Default(),
		}
		c balancersConfig
	)

	for _, o := range opts {
		o(&h)
	}

	if err := json.Unmarshal([]byte(config), &c); err != nil {
		if h.errorHandler != nil {
			h.errorHandler(err)
		}
		return h.fallbackBalancer
	}

	switch c.Type {
	case typeSingle:
		b = SingleConn()
	case typeRandomChoice:
		b = RandomChoice()
	case typeRoundRobin:
		b = RoundRobin()
	default:
		if h.errorHandler != nil {
			h.errorHandler(errors.Errorf("unknown type of balancer: %s", c.Type))
		}
		return h.fallbackBalancer
	}

	switch c.Prefer {
	case preferLocalDC:
		if c.Fallback {
			return PreferLocalDCWithFallBack(b)
		}
		return PreferLocalDC(b)
	case preferLocations:
		if len(c.Locations) == 0 {
			if h.errorHandler != nil {
				h.errorHandler(errors.Errorf("empty locations list in balancer '%s' config", c.Type))
			}
			return h.fallbackBalancer
		}
		if c.Fallback {
			return PreferLocationsWithFallback(b, c.Locations...)
		}
		return PreferLocations(b, c.Locations...)
	default:
		return b
	}
}
