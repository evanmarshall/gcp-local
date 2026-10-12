package vertexai

import (
	"encoding/json"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Token counts are a local approximation, not Gemini's tokenizer: each
// whitespace-separated word counts one token per started four runes.
// countTokens always uses this rule; usageMetadata uses it for the stub and
// whenever Ollama reports no counts.

// wordTokens is the approximate token cost of one word.
func wordTokens(word string) int { return (utf8.RuneCountInString(word) + 3) / 4 }

// textTokens approximates the token count of one text.
func textTokens(text string) int {
	count := 0
	for word := range strings.FieldsSeq(text) {
		count += wordTokens(word)
	}
	return count
}

// jsonText renders a value as compact JSON, or "{}" if it cannot be encoded.
func jsonText(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(encoded)
}

// partTokens counts one part. Inline data counts one token per 1 KiB.
func partTokens(part Part) int {
	count := textTokens(part.Text)
	if part.InlineData != nil {
		count += max(1, len(part.InlineData.Data)/1024)
	}
	if part.FunctionCall != nil {
		count += textTokens(jsonText(part.FunctionCall))
	}
	if part.FunctionResponse != nil {
		count += textTokens(jsonText(part.FunctionResponse))
	}
	return count
}

// countRequestTokens counts the prompt: contents, system instruction, and
// function declarations.
func countRequestTokens(request *GenerateRequest) int {
	count := 0
	for _, content := range request.turns() {
		for _, part := range content.Parts {
			count += partTokens(part)
		}
	}
	for _, declaration := range request.declarations() {
		count += textTokens(jsonText(declaration))
	}
	return count
}

// billableCharacters counts the non-whitespace characters of every text part.
func billableCharacters(request *GenerateRequest) int {
	count := 0
	for _, content := range request.turns() {
		for _, part := range content.Parts {
			for _, r := range part.Text {
				if !unicode.IsSpace(r) {
					count++
				}
			}
		}
	}
	return count
}

// truncateTokens cuts text to at most limit approximate tokens.
func truncateTokens(text string, limit int) (string, bool) {
	if textTokens(text) <= limit {
		return text, false
	}
	var kept []string
	used := 0
	for word := range strings.FieldsSeq(text) {
		if used+wordTokens(word) > limit {
			break
		}
		kept = append(kept, word)
		used += wordTokens(word)
	}
	return strings.Join(kept, " "), true
}
