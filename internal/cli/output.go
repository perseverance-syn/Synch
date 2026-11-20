package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
)

// errWrite marks a failure writing results, which ends streaming commands.
var errWrite = errors.New("output closed")

// printer renders results as JSON or as readable text. Write failures are
// returned so long-running commands stop instead of spinning.
type printer struct {
	stdout io.Writer
	stderr io.Writer
	json   bool
}

// rawBody prints the server's bytes unchanged, which keeps --json output
// byte-faithful to the API response.
func (p *printer) rawBody(resp *response) error {
	if p.json {
		if _, err := fmt.Fprintf(p.stdout, "%s\n", strings.TrimRight(string(resp.body), "\n")); err != nil {
			return err
		}
		return nil
	}
	return p.textValue(resp.body)
}

// event prints one stream event: the raw event JSON as one line in --json
// mode, or a cursor-and-type row for readers. Write failures are wrapped in
// errWrite so the follower ends instead of spinning on a closed output.
func (p *printer) event(cursor int64, eventType string, data []byte) error {
	if p.json {
		if _, err := fmt.Fprintf(p.stdout, "%s\n", data); err != nil {
			return fmt.Errorf("%w: %v", errWrite, err)
		}
		return nil
	}
	if _, err := fmt.Fprintf(p.stdout, "%d\t%s\n", cursor, eventType); err != nil {
		return fmt.Errorf("%w: %v", errWrite, err)
	}
	return nil
}

// textValue renders one JSON document as key: value lines for readers.
func (p *printer) textValue(body []byte) error {
	var value any
	if err := json.Unmarshal(body, &value); err != nil {
		_, werr := fmt.Fprintf(p.stdout, "%s\n", body)
		return werr
	}
	var out strings.Builder
	renderValue(&out, value, 0)
	_, err := fmt.Fprint(p.stdout, out.String())
	return err
}

// renderValue writes nested JSON as indented key: value lines, keeping
// arrays and nested objects as compact JSON on their key's line.
func renderValue(out *strings.Builder, value any, depth int) {
	indent := strings.Repeat("  ", depth)
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if nested, ok := typed[key].(map[string]any); ok {
				fmt.Fprintf(out, "%s%s:\n", indent, key)
				renderValue(out, nested, depth+1)
				continue
			}
			fmt.Fprintf(out, "%s%s: %s\n", indent, key, compactJSON(typed[key]))
		}
	default:
		fmt.Fprintf(out, "%s%s\n", indent, compactJSON(value))
	}
}

// compactJSON encodes one value without whitespace for a single line.
func compactJSON(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%v", value)
	}
	return string(raw)
}

// receipt prints a mutating call's outcome: the receipt id and whether the
// server replayed a stored response (receipts.md section 4). In --json mode
// the stored receipt body is printed with the replayed flag added, because
// the flag arrives only as a header.
func (p *printer) receipt(resp *response) error {
	if p.json {
		var value map[string]any
		if err := json.Unmarshal(resp.body, &value); err != nil {
			return p.rawBody(resp)
		}
		value["replayed"] = resp.replayed == "true"
		raw, err := json.Marshal(value)
		if err != nil {
			return p.rawBody(resp)
		}
		_, err = fmt.Fprintf(p.stdout, "%s\n", raw)
		return err
	}
	fields := map[string]any{}
	_ = json.Unmarshal(resp.body, &fields)
	var out strings.Builder
	fmt.Fprintf(&out, "receipt %s status %s http %d replayed %s\n",
		stringField(fields, "receipt_id"), stringField(fields, "status"), resp.status, replayedText(resp.replayed))
	if result, ok := fields["result"].(map[string]any); ok {
		renderValue(&out, result, 1)
	}
	_, err := fmt.Fprint(p.stdout, out.String())
	return err
}

// printThreadRows renders a threads list as one row per thread in text mode;
// --json passes the server's document through unchanged.
func (p *printer) printThreadRows(resp *response) error {
	if p.json {
		return p.rawBody(resp)
	}
	var page struct {
		Threads []struct {
			ID        string `json:"id"`
			ProjectID string `json:"project_id"`
			Role      string `json:"role"`
			State     string `json:"state"`
			Depth     int    `json:"depth"`
		} `json:"threads"`
	}
	if err := json.Unmarshal(resp.body, &page); err != nil {
		return p.textValue(resp.body)
	}
	var out strings.Builder
	out.WriteString("ID\tPROJECT\tROLE\tSTATE\tDEPTH\n")
	for _, thread := range page.Threads {
		fmt.Fprintf(&out, "%s\t%s\t%s\t%s\t%d\n", thread.ID, thread.ProjectID, thread.Role, thread.State, thread.Depth)
	}
	_, err := fmt.Fprint(p.stdout, out.String())
	return err
}

// stringField reads one string member without failing on absent members.
func stringField(fields map[string]any, key string) string {
	value, _ := fields[key].(string)
	return value
}

// replayedText normalizes an absent replayed header to false for display.
func replayedText(header string) string {
	if header == "true" {
		return "true"
	}
	return "false"
}
