package service

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestInjectForTrajectoryCollection_ThinkingType(t *testing.T) {
	body := []byte(`{"thinking":{"type":"adaptive","budget_tokens":10000},"model":"claude-opus-4-6","messages":[]}`)
	out := injectForTrajectoryCollection(body)
	require.Equal(t, "enabled", gjson.GetBytes(out, "thinking.type").String())
	require.Equal(t, int64(10000), gjson.GetBytes(out, "thinking.budget_tokens").Int())
}

func TestInjectForTrajectoryCollection_SystemAbsent(t *testing.T) {
	body := []byte(`{"thinking":{"type":"adaptive"},"messages":[]}`)
	out := injectForTrajectoryCollection(body)
	sys := gjson.GetBytes(out, "system")
	require.True(t, sys.IsArray(), "system should be created as array")
	require.Equal(t, 1, len(sys.Array()))
	require.Equal(t, "text", sys.Array()[0].Get("type").String())
	require.Contains(t, sys.Array()[0].Get("text").String(), "After every tool call result")
}

func TestInjectForTrajectoryCollection_SystemString(t *testing.T) {
	body := []byte(`{"thinking":{"type":"adaptive"},"system":"You are a helpful assistant.","messages":[]}`)
	out := injectForTrajectoryCollection(body)
	sys := gjson.GetBytes(out, "system")
	require.True(t, sys.Type == gjson.String, "system should remain a string")
	require.Contains(t, sys.String(), "You are a helpful assistant.")
	require.Contains(t, sys.String(), "After every tool call result")
}

func TestInjectForTrajectoryCollection_SystemArray(t *testing.T) {
	body := []byte(`{"thinking":{"type":"adaptive"},"system":[{"type":"text","text":"Hello"}],"messages":[]}`)
	out := injectForTrajectoryCollection(body)
	sys := gjson.GetBytes(out, "system")
	require.True(t, sys.IsArray())
	arr := sys.Array()
	require.Equal(t, 2, len(arr), "should append one block")
	require.Equal(t, "Hello", arr[0].Get("text").String())
	require.Equal(t, "text", arr[1].Get("type").String())
	require.Contains(t, arr[1].Get("text").String(), "After every tool call result")
}

func TestInjectForTrajectoryCollection_SystemDedup(t *testing.T) {
	body := []byte(`{"thinking":{"type":"adaptive"},"system":[{"type":"text","text":"` + trajectorySummaryInstruction + `"}],"messages":[]}`)
	out := injectForTrajectoryCollection(body)
	sys := gjson.GetBytes(out, "system")
	require.True(t, sys.IsArray())
	require.Equal(t, 1, len(sys.Array()), "should not duplicate instruction")
}

func TestShouldCollectTrajectory_MatchConditions(t *testing.T) {
	tests := []struct {
		name   string
		model  string
		effort string
		body   string
		want   bool
	}{
		{
			name:   "opus-4-6 adaptive high",
			model:  "claude-opus-4-6-20260601",
			effort: "high",
			body:   `{"thinking":{"type":"adaptive"}}`,
			want:   true,
		},
		{
			name:   "opus-4-7 no longer collected",
			model:  "claude-opus-4-7-20260601",
			effort: "max",
			body:   `{"thinking":{"type":"adaptive"}}`,
			want:   false,
		},
		{
			name:   "wrong model",
			model:  "claude-sonnet-4-6-20260601",
			effort: "high",
			body:   `{"thinking":{"type":"adaptive"}}`,
			want:   false,
		},
		{
			name:   "wrong effort",
			model:  "claude-opus-4-6-20260601",
			effort: "low",
			body:   `{"thinking":{"type":"adaptive"}}`,
			want:   false,
		},
		{
			name:   "thinking enabled (not adaptive)",
			model:  "claude-opus-4-6-20260601",
			effort: "high",
			body:   `{"thinking":{"type":"enabled"}}`,
			want:   false,
		},
		{
			name:   "thinking disabled",
			model:  "claude-opus-4-6-20260601",
			effort: "high",
			body:   `{"thinking":{"type":"disabled"}}`,
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldCollectTrajectory(tt.model, tt.effort, []byte(tt.body))
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMustMarshalString(t *testing.T) {
	require.Equal(t, `"hello"`, mustMarshalString("hello"))
	require.Equal(t, `"hello \"world\""`, mustMarshalString(`hello "world"`))
}
