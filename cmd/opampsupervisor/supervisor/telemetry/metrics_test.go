// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
)

func newTestMetrics(t *testing.T) (*Metrics, *sdkmetric.ManualReader, *time.Time) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(t.Context()) })

	m, err := NewMetrics(mp)
	require.NoError(t, err)

	now := time.Unix(1000, 0)
	m.now = func() time.Time { return now }
	return m, reader, &now
}

func collect(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	require.Len(t, rm.ScopeMetrics, 1)
	out := map[string]metricdata.Metrics{}
	for _, m := range rm.ScopeMetrics[0].Metrics {
		out[m.Name] = m
	}
	return out
}

func sum(points ...metricdata.DataPoint[int64]) metricdata.Sum[int64] {
	return metricdata.Sum[int64]{
		DataPoints:  points,
		Temporality: metricdata.CumulativeTemporality,
		IsMonotonic: true,
	}
}

func upDown(value int64) metricdata.Sum[int64] {
	return metricdata.Sum[int64]{
		DataPoints:  []metricdata.DataPoint[int64]{{Value: value}},
		Temporality: metricdata.CumulativeTemporality,
		IsMonotonic: false,
	}
}

func point(value int64, kv ...attribute.KeyValue) metricdata.DataPoint[int64] {
	return metricdata.DataPoint[int64]{Attributes: attribute.NewSet(kv...), Value: value}
}

func TestOpAMPConnectionMetrics(t *testing.T) {
	m, reader, now := newTestMetrics(t)
	ctx := t.Context()

	// Connection status starts at 0 and is exported right away.
	got := collect(t, reader)
	metricdatatest.AssertAggregationsEqual(t, upDown(0), got[OpAMPConnectionStatusMetric].Data, metricdatatest.IgnoreTimestamp())

	m.RecordOpAMPConnectFailed(ctx)
	m.RecordOpAMPConnected(ctx)
	// The HTTP client reports OnConnect for every successful request; it must not
	// be counted as a new connection.
	m.RecordOpAMPConnected(ctx)

	got = collect(t, reader)
	metricdatatest.AssertAggregationsEqual(t, upDown(1), got[OpAMPConnectionStatusMetric].Data, metricdatatest.IgnoreTimestamp())
	metricdatatest.AssertAggregationsEqual(t, sum(
		point(1, attribute.String("outcome", "failure")),
		point(1, attribute.String("outcome", "success")),
	), got[OpAMPConnectionAttemptsMetric].Data, metricdatatest.IgnoreTimestamp())
	_, ok := got[OpAMPDisconnectsMetric]
	require.False(t, ok, "no disconnect expected yet")

	*now = now.Add(90 * time.Second)
	m.RecordOpAMPDisconnected(ctx)
	// A second disconnect without a new connection is ignored.
	m.RecordOpAMPDisconnected(ctx)

	got = collect(t, reader)
	metricdatatest.AssertAggregationsEqual(t, upDown(0), got[OpAMPConnectionStatusMetric].Data, metricdatatest.IgnoreTimestamp())
	metricdatatest.AssertAggregationsEqual(t, sum(point(1)), got[OpAMPDisconnectsMetric].Data, metricdatatest.IgnoreTimestamp())

	hist, ok := got[OpAMPConnectionDurationMetric].Data.(metricdata.Histogram[float64])
	require.True(t, ok)
	require.Len(t, hist.DataPoints, 1)
	require.Equal(t, uint64(1), hist.DataPoints[0].Count)
	require.InDelta(t, 90.0, hist.DataPoints[0].Sum, 0.001)
}

func TestOpAMPConnectFailedWhileConnectedCountsAsDisconnect(t *testing.T) {
	m, reader, now := newTestMetrics(t)
	ctx := t.Context()

	m.RecordOpAMPConnected(ctx)
	*now = now.Add(5 * time.Second)
	// HTTP transport: a failing poll after successful ones means the server is gone.
	m.RecordOpAMPConnectFailed(ctx)

	got := collect(t, reader)
	metricdatatest.AssertAggregationsEqual(t, upDown(0), got[OpAMPConnectionStatusMetric].Data, metricdatatest.IgnoreTimestamp())
	metricdatatest.AssertAggregationsEqual(t, sum(point(1)), got[OpAMPDisconnectsMetric].Data, metricdatatest.IgnoreTimestamp())
}

func TestOpAMPMessageMetrics(t *testing.T) {
	m, reader, _ := newTestMetrics(t)
	ctx := t.Context()

	m.RecordOpAMPMessageSent(ctx, 100)
	m.RecordOpAMPMessageSent(ctx, 50)
	m.RecordOpAMPMessageReceived(ctx, 20)

	got := collect(t, reader)
	transmit := attribute.String("network.io.direction", "transmit")
	receive := attribute.String("network.io.direction", "receive")
	metricdatatest.AssertAggregationsEqual(t, sum(
		point(2, transmit),
		point(1, receive),
	), got[OpAMPMessagesMetric].Data, metricdatatest.IgnoreTimestamp())
	metricdatatest.AssertAggregationsEqual(t, sum(
		point(150, transmit),
		point(20, receive),
	), got[OpAMPNetworkIOMetric].Data, metricdatatest.IgnoreTimestamp())
}
