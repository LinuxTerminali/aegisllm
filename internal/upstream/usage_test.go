package upstream

import "testing"

func TestParseUsageTokens(t *testing.T) {
	cases := []struct {
		name string
		body string
		want int
	}{
		{"openai total", `{"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}`, 15},
		{"openai no total", `{"usage":{"prompt_tokens":10,"completion_tokens":5}}`, 15},
		{"anthropic", `{"id":"msg_1","usage":{"input_tokens":120,"output_tokens":30}}`, 150},
		{"missing", `{"id":"x"}`, 0},
		{"garbage", `not json`, 0},
	}
	for _, c := range cases {
		if got := ParseUsageTokens([]byte(c.body)); got != c.want {
			t.Errorf("%s: want %d got %d", c.name, c.want, got)
		}
	}
}
