package scanner

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

// decodeValue reads exactly one JSON value while preserving numbers for redaction.
func decodeValue(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if decoder.Decode(&value) != nil || !errors.Is(decoder.Decode(new(any)), io.EOF) {
		return nil, ErrReport
	}
	return value, nil
}

// redactJSON redacts values and dynamic object keys before they can be retained.
func (d *Driver) redactJSON(value any) (any, error) {
	switch value := value.(type) {
	case string:
		return d.redactText(value)
	case json.Number:
		redacted, err := d.redactText(value.String())
		if redacted != value.String() || err != nil {
			return redacted, err
		}
	case []any:
		for i, child := range value {
			redacted, err := d.redactJSON(child)
			if err != nil {
				return nil, err
			}
			value[i] = redacted
		}
	case map[string]any:
		result := make(map[string]any, len(value))
		for key, child := range value {
			safeKey, err := d.redactText(key)
			if err != nil {
				return nil, err
			}
			if _, exists := result[safeKey]; exists {
				return nil, ErrRedaction
			}
			redacted, err := d.redactJSON(child)
			if err != nil {
				return nil, err
			}
			result[safeKey] = redacted
		}
		return result, nil
	}
	return value, nil
}

// redactText hides redactor diagnostics because they may include raw values.
func (d *Driver) redactText(value string) (string, error) {
	safe, err := d.redact(value)
	if err != nil {
		return "", ErrRedaction
	}
	return safe, nil
}
