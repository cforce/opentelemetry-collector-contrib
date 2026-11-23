// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	CollectorHealthStatusMetric   = "supervisor.agent.health_status"
	CollectorFallbackStatusMetric = "supervisor.agent.fallback_status"

	OpAMPConnectionStatusMetric   = "supervisor.opamp.connection.status"
	OpAMPConnectionAttemptsMetric = "supervisor.opamp.connection.attempts"
	OpAMPDisconnectsMetric        = "supervisor.opamp.disconnects"
	OpAMPConnectionDurationMetric = "supervisor.opamp.connection.duration"
	OpAMPMessagesMetric           = "supervisor.opamp.messages"
	OpAMPNetworkIOMetric          = "supervisor.opamp.network.io"
)

var (
	outcomeSuccess = attribute.NewSet(attribute.String("outcome", "success"))
	outcomeFailure = attribute.NewSet(attribute.String("outcome", "failure"))

	directionSent     = attribute.NewSet(attribute.String("network.io.direction", "transmit"))
	directionReceived = attribute.NewSet(attribute.String("network.io.direction", "receive"))
)

type Metrics struct {
	collectorHealthStatusMetric   metric.Int64UpDownCounter
	collectorFallbackStatusMetric metric.Int64UpDownCounter
	healthStatus                  bool
	fallbackStatus                bool

	opampConnectionStatus   metric.Int64UpDownCounter
	opampConnectionAttempts metric.Int64Counter
	opampDisconnects        metric.Int64Counter
	opampConnectionDuration metric.Float64Histogram
	opampMessages           metric.Int64Counter
	opampNetworkIO          metric.Int64Counter

	// connMu guards the OpAMP connection state below. The OpAMP client invokes
	// its callbacks from different goroutines.
	connMu      sync.Mutex
	connected   bool
	connectedAt time.Time
	now         func() time.Time
}

func NewMetrics(meterProvider metric.MeterProvider) (*Metrics, error) {
	meter := meterProvider.Meter("opamp-supervisor")

	m := &Metrics{now: time.Now}
	var errs, err error

	m.collectorHealthStatusMetric, err = meter.Int64UpDownCounter(
		CollectorHealthStatusMetric,
		metric.WithDescription("Current health status of the collector (1=healthy, 0=unhealthy)"),
	)
	errs = errors.Join(errs, err)

	m.collectorFallbackStatusMetric, err = meter.Int64UpDownCounter(
		CollectorFallbackStatusMetric,
		metric.WithDescription("Current fallback status of the collector (1=fallback active, 0=fallback inactive)"),
	)
	errs = errors.Join(errs, err)

	m.opampConnectionStatus, err = meter.Int64UpDownCounter(
		OpAMPConnectionStatusMetric,
		metric.WithDescription("Current status of the connection to the OpAMP server (1=connected, 0=disconnected)"),
		metric.WithUnit("{connection}"),
	)
	errs = errors.Join(errs, err)

	m.opampConnectionAttempts, err = meter.Int64Counter(
		OpAMPConnectionAttemptsMetric,
		metric.WithDescription("Number of attempts to connect to the OpAMP server, by outcome"),
		metric.WithUnit("{attempt}"),
	)
	errs = errors.Join(errs, err)

	m.opampDisconnects, err = meter.Int64Counter(
		OpAMPDisconnectsMetric,
		metric.WithDescription("Number of times an established connection to the OpAMP server was lost"),
		metric.WithUnit("{disconnect}"),
	)
	errs = errors.Join(errs, err)

	m.opampConnectionDuration, err = meter.Float64Histogram(
		OpAMPConnectionDurationMetric,
		metric.WithDescription("Duration of established connections to the OpAMP server, recorded when the connection is lost"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(1, 10, 60, 300, 900, 1800, 3600, 7200, 14400, 28800, 86400),
	)
	errs = errors.Join(errs, err)

	m.opampMessages, err = meter.Int64Counter(
		OpAMPMessagesMetric,
		metric.WithDescription("Number of OpAMP messages exchanged with the OpAMP server, by direction"),
		metric.WithUnit("{message}"),
	)
	errs = errors.Join(errs, err)

	m.opampNetworkIO, err = meter.Int64Counter(
		OpAMPNetworkIOMetric,
		metric.WithDescription("Size of the encoded OpAMP messages exchanged with the OpAMP server, by direction, before transport compression"),
		metric.WithUnit("By"),
	)
	errs = errors.Join(errs, err)

	if errs != nil {
		return nil, errs
	}

	// Initialize metrics to 0 to ensure they are exported
	m.collectorHealthStatusMetric.Add(context.Background(), 0)
	m.opampConnectionStatus.Add(context.Background(), 0)
	return m, nil
}

func (m *Metrics) SetCollectorHealthStatus(ctx context.Context, healthy bool) {
	// Only update the metric if the health status has changed
	if !m.healthStatus && healthy {
		m.collectorHealthStatusMetric.Add(ctx, 1)
		m.healthStatus = healthy
	} else if m.healthStatus && !healthy {
		m.collectorHealthStatusMetric.Add(ctx, -1)
		m.healthStatus = healthy
	}
}

func (m *Metrics) SetCollectorFallbackStatus(ctx context.Context, fallbackActive bool) {
	// Only update the metric if the fallback status has changed
	if !m.fallbackStatus && fallbackActive {
		m.collectorFallbackStatusMetric.Add(ctx, 1)
		m.fallbackStatus = fallbackActive
	} else if m.fallbackStatus && !fallbackActive {
		m.collectorFallbackStatusMetric.Add(ctx, -1)
		m.fallbackStatus = fallbackActive
	}
}

// RecordOpAMPConnected records that the OpAMP client reported a successful
// connection. The HTTP client reports this for every successful request, so
// only a transition from disconnected to connected counts as a new connection.
func (m *Metrics) RecordOpAMPConnected(ctx context.Context) {
	m.connMu.Lock()
	defer m.connMu.Unlock()
	if m.connected {
		return
	}
	m.connected = true
	m.connectedAt = m.now()
	m.opampConnectionAttempts.Add(ctx, 1, metric.WithAttributeSet(outcomeSuccess))
	m.opampConnectionStatus.Add(ctx, 1)
}

// RecordOpAMPConnectFailed records a failed attempt to reach the OpAMP server.
// If the supervisor was considered connected, which happens when an HTTP
// request fails after earlier ones succeeded, the connection is also
// recorded as lost.
func (m *Metrics) RecordOpAMPConnectFailed(ctx context.Context) {
	m.connMu.Lock()
	defer m.connMu.Unlock()
	m.opampConnectionAttempts.Add(ctx, 1, metric.WithAttributeSet(outcomeFailure))
	m.disconnectLocked(ctx)
}

// RecordOpAMPDisconnected records that an established connection to the
// OpAMP server was lost.
func (m *Metrics) RecordOpAMPDisconnected(ctx context.Context) {
	m.connMu.Lock()
	defer m.connMu.Unlock()
	m.disconnectLocked(ctx)
}

func (m *Metrics) disconnectLocked(ctx context.Context) {
	if !m.connected {
		return
	}
	m.connected = false
	m.opampDisconnects.Add(ctx, 1)
	m.opampConnectionStatus.Add(ctx, -1)
	m.opampConnectionDuration.Record(ctx, m.now().Sub(m.connectedAt).Seconds())
}

// RecordOpAMPMessageSent records an AgentToServer message of the given encoded
// size that was handed to the transport.
func (m *Metrics) RecordOpAMPMessageSent(ctx context.Context, size int) {
	m.opampMessages.Add(ctx, 1, metric.WithAttributeSet(directionSent))
	m.opampNetworkIO.Add(ctx, int64(size), metric.WithAttributeSet(directionSent))
}

// RecordOpAMPMessageReceived records a ServerToAgent message of the given
// encoded size that was received from the server.
func (m *Metrics) RecordOpAMPMessageReceived(ctx context.Context, size int) {
	m.opampMessages.Add(ctx, 1, metric.WithAttributeSet(directionReceived))
	m.opampNetworkIO.Add(ctx, int64(size), metric.WithAttributeSet(directionReceived))
}
