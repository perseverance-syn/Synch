package daemon

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	sctx "synch/internal/context"
	"synch/internal/engine"
	"synch/internal/roles"
	"synch/internal/thread"
)

// --- Role skills: role guidance and pinned skills for every session start (card 301) ---

// roleOf returns the role of the child's recorded assignment (decision 06), or the Builder
// role for a thread without one. Loading a role never changes the thread's role in policy.
func (d *Daemon) roleOf(ctx context.Context, threadID string) string {
	a, err := d.recorder.Assignment(ctx, threadID)
	if err != nil || a.Role == "" {
		return builderRole
	}
	return a.Role
}

// roleBlock renders the role's instructions as a stable guidance block for context.Assemble.
// It is empty when no role pack is configured. The text only instructs; tools, grants
// and approvals still come from policy.
func (d *Daemon) roleBlock(role string) ([]sctx.Block, error) {
	if d.roles == nil {
		return nil, nil
	}
	r, err := d.roles.Role(role)
	if err != nil {
		return nil, err
	}
	text := r.Instructions + "\nPolicy profile: " + r.Profile + ". Tools, grants and approvals come from Synch policy; skills never widen them."
	return []sctx.Block{{Kind: "role", Source: "role:" + r.Name, Digest: fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(text))), Text: text, Stable: true}}, nil
}

// skillsFor selects the skills a session starts with and the pins to record. A session that
// continues a checkpoint (rollover or harness handoff) resolves the checkpoint's pins, so it
// keeps the text the assignment started with or fails closed with roles.ErrDigestMismatch. A
// fresh assignment pins every eligible skill of its role now. With no role pack it selects none.
func (d *Daemon) skillsFor(role string, cp *thread.Checkpoint) ([]engine.PinnedSkill, []thread.Skill, error) {
	pins := []thread.Skill{}
	if cp != nil && len(cp.Skills) > 0 {
		if d.roles == nil {
			return nil, nil, errors.New("checkpoint pins skills but no role pack is configured")
		}
		want := make([]roles.Pin, 0, len(cp.Skills))
		for _, s := range cp.Skills {
			want = append(want, roles.Pin{ID: s.ID, Digest: s.Digest})
		}
		skills, err := d.roles.Resolve(role, want)
		return roles.Engine(skills), cp.Skills, err
	}
	if d.roles == nil {
		return nil, pins, nil
	}
	r, err := d.roles.Role(role)
	if err != nil {
		return nil, nil, err
	}
	for _, s := range r.Skills {
		pins = append(pins, thread.Skill{ID: s.ID, Digest: s.Digest})
	}
	return roles.Engine(r.Skills), pins, nil
}

// pinPayload renders pins as the "skills" array of assignment.dispatched.
func pinPayload(pins []thread.Skill) []any {
	out := make([]any, 0, len(pins))
	for _, p := range pins {
		out = append(out, map[string]any{"id": p.ID, "digest": p.Digest})
	}
	return out
}
