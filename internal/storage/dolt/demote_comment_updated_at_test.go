package dolt

import (
	"testing"

	"github.com/steveyegge/beads/internal/types"
)

// TestDemoteToWispCarriesTheCommentStamps is the server-mode half of gf-d7b
// finding 2k for the demotion copy (demoteToWispInTx's INSERT IGNORE INTO
// wisp_comments ... SELECT ... FROM comments): a comment moved to the wisp
// plane is the same comment, so it keeps the created_at and updated_at it was
// written with instead of taking wisp_comments.updated_at's CURRENT_TIMESTAMP
// default on the way over.
func TestDemoteToWispCarriesTheCommentStamps(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx, cancel := testContext(t)
	defer cancel()

	issue := &types.Issue{
		Title:     "will be demoted with a comment",
		Status:    types.StatusOpen,
		Priority:  2,
		IssueType: types.TypeTask,
	}
	if err := store.CreateIssue(ctx, issue, "tester"); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	const stamp = "2025-01-02 03:04:05"
	if _, err := store.db.ExecContext(ctx,
		"INSERT INTO comments (id, issue_id, author, text, created_at, updated_at) VALUES ('cmt-demote', ?, 'gus', 'demoting', ?, ?)",
		issue.ID, stamp, stamp); err != nil {
		t.Fatalf("seed comment: %v", err)
	}

	if err := store.UpdateIssue(ctx, issue.ID, map[string]interface{}{"wisp": true}, "tester"); err != nil {
		t.Fatalf("UpdateIssue with wisp=true: %v", err)
	}
	if !store.isActiveWisp(ctx, issue.ID) {
		t.Fatalf("issue %s should be in the wisps table after the demotion", issue.ID)
	}

	var created, updated string
	if err := store.db.QueryRowContext(ctx,
		"SELECT CAST(created_at AS CHAR), CAST(updated_at AS CHAR) FROM wisp_comments WHERE id = 'cmt-demote' AND issue_id = ?",
		issue.ID).Scan(&created, &updated); err != nil {
		t.Fatalf("read the demoted comment: %v", err)
	}
	if created != stamp || updated != stamp {
		t.Errorf("demoted comment stamps = (%s, %s), want (%s, %s): the copy must carry updated_at, not take the default", created, updated, stamp, stamp)
	}
	var left int
	if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM comments WHERE issue_id = ?", issue.ID).Scan(&left); err != nil {
		t.Fatalf("count issue-plane comments: %v", err)
	}
	if left != 0 {
		t.Errorf("%d comment row(s) left in comments after the demotion, want 0", left)
	}
}
