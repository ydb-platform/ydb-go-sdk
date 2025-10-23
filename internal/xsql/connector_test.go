package xsql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEngine_String(t *testing.T) {
	tests := []struct {
		name     string
		engine   Engine
		expected string
	}{
		{
			name:     "QUERY",
			engine:   QUERY,
			expected: "QUERY",
		},
		{
			name:     "TABLE",
			engine:   TABLE,
			expected: "TABLE",
		},
		{
			name:     "Unknown",
			engine:   Engine(99),
			expected: "UNKNOWN",
		},
		{
			name:     "Zero",
			engine:   Engine(0),
			expected: "UNKNOWN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.engine.String()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestConnector_Getters(t *testing.T) {
	t.Run("ParentGetter", func(t *testing.T) {
		connector := &Connector{
			parent: nil,
		}
		require.Nil(t, connector.Parent())
	})

	t.Run("RetryBudgetGetter", func(t *testing.T) {
		connector := &Connector{
			retryBudget: nil,
		}
		require.Nil(t, connector.RetryBudget())
	})

	t.Run("BindingsGetter", func(t *testing.T) {
		connector := &Connector{
			bindings: nil,
		}
		require.Nil(t, connector.Bindings())
	})

	t.Run("ClockGetter", func(t *testing.T) {
		connector := &Connector{
			clock: nil,
		}
		require.Nil(t, connector.Clock())
	})

	t.Run("TraceGetter", func(t *testing.T) {
		connector := &Connector{
			trace: nil,
		}
		require.Nil(t, connector.Trace())
	})

	t.Run("TraceRetryGetter", func(t *testing.T) {
		connector := &Connector{
			traceRetry: nil,
		}
		require.Nil(t, connector.TraceRetry())
	})
}

func TestConnector_Driver(t *testing.T) {
	connector := &Connector{}
	driver := connector.Driver()
	require.Equal(t, connector, driver)
}

func TestConnector_Close(t *testing.T) {
	t.Run("FirstClose", func(t *testing.T) {
		connector := &Connector{
			done: make(chan struct{}),
		}
		err := connector.Close()
		require.NoError(t, err)

		select {
		case <-connector.done:
			// Channel should be closed
		default:
			t.Fatal("done channel should be closed")
		}
	})

	t.Run("SecondClose", func(t *testing.T) {
		connector := &Connector{
			done: make(chan struct{}),
		}
		err := connector.Close()
		require.NoError(t, err)

		err = connector.Close()
		require.NoError(t, err)
	})

	t.Run("WithOnCloseCallbacks", func(t *testing.T) {
		called := false
		connector := &Connector{
			done: make(chan struct{}),
			onClose: []func(*Connector){
				func(c *Connector) {
					called = true
				},
			},
		}
		err := connector.Close()
		require.NoError(t, err)
		require.True(t, called)
	})
}
