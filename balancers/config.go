package balancers

import (
	"encoding/json"
	"fmt"

	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
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
	fallbackBalancer *balancerConfig.Config
	errorHandler     func(error)
}

type fromConfigOption func(h *fromConfigOptionsHolder)

func WithParseErrorFallbackBalancer(b *balancerConfig.Config) fromConfigOption {
	return func(h *fromConfigOptionsHolder) {
		h.fallbackBalancer = b
	}
}

func WithParseErrorHandler(errorHandler func(error)) fromConfigOption {
	return func(h *fromConfigOptionsHolder) {
		h.errorHandler = errorHandler
	}
}

func CreateFromConfig(config string) (*balancerConfig.Config, error) {
	var (
		b   *balancerConfig.Config
		err error
		c   balancersConfig
	)

	if err = json.Unmarshal([]byte(config), &c); err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	switch c.Type {
	case typeSingle:
		b = SingleConn()
	case typeRandomChoice:
		b = RandomChoice()
	case typeRoundRobin:
		b = RoundRobin()
	default:
		return nil, xerrors.WithStackTrace(fmt.Errorf("unknown type of balancer: %s", c.Type))
	}

	switch c.Prefer {
	case preferLocalDC:
		if c.Fallback {
			return PreferLocalDCWithFallBack(b), nil
		}
		return PreferLocalDC(b), nil
	case preferLocations:
		if len(c.Locations) == 0 {
			return nil, xerrors.WithStackTrace(fmt.Errorf("empty locations list in balancer '%s' config", c.Type))
		}
		if c.Fallback {
			return PreferLocationsWithFallback(b, c.Locations...), nil
		}
		return PreferLocations(b, c.Locations...), nil
	default:
		return b, nil
	}
}

func FromConfig(config string, opts ...fromConfigOption) *balancerConfig.Config {
	var (
		h = fromConfigOptionsHolder{
			fallbackBalancer: Default(),
		}
		b   *balancerConfig.Config
		err error
	)
	for _, o := range opts {
		o(&h)
	}

	b, err = CreateFromConfig(config)
	if err != nil {
		if h.errorHandler != nil {
			h.errorHandler(err)
		}
		return h.fallbackBalancer
	}

	return b
}
