//go:build cgo

package embeddeddolt_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/storage/domain"
	"github.com/steveyegge/beads/internal/storage/domain/db"
	"github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/storage/versioncontrolops"
	"github.com/steveyegge/beads/internal/types"
)

// TestImportedCommentConvergesAcrossReplicas is the behavioral form of
// gf-d7b finding 2k (the gf-c7n beads-vs-integrated gate's Major at
// 0072_add_comment_external_ref.up.sql:76 with the comment writers): 0072
// gave comments.updated_at a DEFAULT CURRENT_TIMESTAMP, and every comment
// insert path left the column to that default, so the same historical
// comment imported on two replicas at different wall times landed as the
// same content-derived id with two different updated_at values. Their next
// merge conflicted on the comments table, and unionRowIsSafe — which compares
// every column of a union-merged row — refused to resolve it.
//
// Two replicas are two branches of one embedded database: the peer branch is
// cut after the issue is seeded, the same historical comment is imported on
// each side in different wall-clock seconds through the import entry point
// (ImportIssueCommentInTx → InsertDerivedComment, the path `bd import`
// takes), and the peer is merged back through MergeAndSettle, the embedded
// pull's merge, whose union-table resolver is unionRowIsSafe.
func TestImportedCommentConvergesAcrossReplicas(t *testing.T) {
	if os.Getenv("BEADS_TEST_EMBEDDED_DOLT") != "1" {
		t.Skip("set BEADS_TEST_EMBEDDED_DOLT=1 to run embedded dolt tests")
	}
	te := newTestEnv(t, "cmtconv")
	ctx := t.Context()
	conn := openSettleConn(t, ctx, te)
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := conn.ExecContext(ctx, query, args...); err != nil {
			t.Fatalf("exec %.100q: %v", query, err)
		}
	}

	exec("INSERT INTO issues (id, title, description, design, acceptance_criteria, notes, status, priority, issue_type) VALUES ('cv-1', 'converge', '', '', '', '', 'open', 2, 'task')")
	exec("CALL DOLT_COMMIT('-Am', 'seed issue')")
	exec("CALL DOLT_BRANCH('cv-peer', 'HEAD')")

	historical := time.Date(2025, 3, 4, 5, 6, 7, 0, time.UTC)
	importHistorical := func(replica string) string {
		t.Helper()
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("%s: begin: %v", replica, err)
		}
		c, err := issueops.ImportIssueCommentInTx(ctx, tx, "cv-1", "alice", "a comment that already happened", historical)
		if err != nil {
			_ = tx.Rollback()
			t.Fatalf("%s: import comment: %v", replica, err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("%s: commit: %v", replica, err)
		}
		exec("CALL DOLT_COMMIT('-Am', ?)", "import the historical comment on "+replica)
		return c.ID
	}

	idA := importHistorical("main")
	rowsA := readCommentRows(t, ctx, conn, "cv-1")

	// The peer replica imports in a later wall-clock second, which is all a
	// CURRENT_TIMESTAMP default needs to disagree.
	waitForTheNextSecond()
	exec("CALL DOLT_CHECKOUT('cv-peer')")
	idB := importHistorical("peer")
	rowsB := readCommentRows(t, ctx, conn, "cv-1")
	exec("CALL DOLT_CHECKOUT('main')")

	if idA != idB {
		t.Fatalf("the two replicas derived different ids for the same comment: %s vs %s", idA, idB)
	}
	if len(rowsA) != 1 || len(rowsB) != 1 {
		t.Fatalf("each replica should hold exactly one comment: main %v, peer %v", rowsA, rowsB)
	}
	want := fmt.Sprintf("%s|cv-1|alice|a comment that already happened|2025-03-04 05:06:07|2025-03-04 05:06:07|", idA)
	if rowsA[0] != want {
		t.Errorf("main's imported comment row = %q, want %q (updated_at must be the comment's own created_at, not this replica's clock)", rowsA[0], want)
	}
	if rowsA[0] != rowsB[0] {
		t.Errorf("the same historical comment imported on two replicas is two different rows:\n main: %s\n peer: %s", rowsA[0], rowsB[0])
	}

	// The merge of two identical imports is clean: no comments conflict for
	// unionRowIsSafe to refuse, one row after the merge, byte for byte the
	// row each side wrote.
	if err := versioncontrolops.MergeAndSettle(ctx, conn, "cv-peer"); err != nil {
		t.Fatalf("merging the peer replica's import: %v (two imports of the same comment must merge without an operator conflict)", err)
	}
	assertScalar(t, ctx, conn, "SELECT COUNT(*) FROM dolt_conflicts", 0)
	assertScalar(t, ctx, conn, "SELECT COUNT(*) FROM comments WHERE issue_id = 'cv-1'", 1)
	if merged := readCommentRows(t, ctx, conn, "cv-1"); len(merged) != 1 || merged[0] != want {
		t.Errorf("after the merge the comment row = %v, want [%s]", merged, want)
	}
}

// TestEveryCommentWriterStampsUpdatedAtFromCreatedAt pins the rule finding 2k
// settles for every comment insert path, not only the import one: a comment
// row's updated_at is written explicitly and equals its created_at — a
// comment is immutable, no path updates one — so the row is a pure function
// of its content on every replica, as its content-derived id already is. A
// copy between planes (promote, demote, a persistence move) carries the
// source row's stamps unchanged.
func TestEveryCommentWriterStampsUpdatedAtFromCreatedAt(t *testing.T) {
	if os.Getenv("BEADS_TEST_EMBEDDED_DOLT") != "1" {
		t.Skip("set BEADS_TEST_EMBEDDED_DOLT=1 to run embedded dolt tests")
	}
	te := newTestEnv(t, "cmtstamp")
	ctx := t.Context()
	conn := openSettleConn(t, ctx, te)
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := conn.ExecContext(ctx, query, args...); err != nil {
			t.Fatalf("exec %.100q: %v", query, err)
		}
	}
	seedIssue := func(id string) {
		t.Helper()
		exec("INSERT INTO issues (id, title, description, design, acceptance_criteria, notes, status, priority, issue_type) VALUES (?, ?, '', '', '', '', 'open', 2, 'task')", id, id)
	}
	const stamp = "2025-01-02 03:04:05"
	stampTime := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	wantRow := func(id, issueID, author, text string) string {
		return fmt.Sprintf("%s|%s|%s|%s|%s|%s|", id, issueID, author, text, stamp, stamp)
	}

	t.Run("InsertDerivedComment", func(t *testing.T) {
		seedIssue("cw-derived")
		id, existed, err := issueops.InsertDerivedComment(ctx, conn, "comments", "cw-derived", "bob", "derived", stamp)
		if err != nil || existed {
			t.Fatalf("InsertDerivedComment: id=%s existed=%v err=%v", id, existed, err)
		}
		if got := readCommentRows(t, ctx, conn, "cw-derived"); len(got) != 1 || got[0] != wantRow(id, "cw-derived", "bob", "derived") {
			t.Errorf("row = %v, want [%s]", got, wantRow(id, "cw-derived", "bob", "derived"))
		}
	})

	t.Run("PersistComments with an incoming id", func(t *testing.T) {
		seedIssue("cw-incoming")
		issue := &types.Issue{ID: "cw-incoming", Comments: []*types.Comment{{ID: "cmt-incoming", Author: "carol", Text: "interchange", CreatedAt: stampTime}}}
		if _, err := issueops.PersistComments(ctx, conn, issue); err != nil {
			t.Fatalf("PersistComments: %v", err)
		}
		if got := readCommentRows(t, ctx, conn, "cw-incoming"); len(got) != 1 || got[0] != wantRow("cmt-incoming", "cw-incoming", "carol", "interchange") {
			t.Errorf("row = %v, want [%s]", got, wantRow("cmt-incoming", "cw-incoming", "carol", "interchange"))
		}
	})

	t.Run("CommentSQLRepository.InsertRecord with an explicit id", func(t *testing.T) {
		seedIssue("cw-repo")
		repo := db.NewCommentSQLRepository(conn)
		if _, err := repo.InsertRecord(ctx, &types.Comment{ID: "cmt-repo", IssueID: "cw-repo", Author: "dave", Text: "repository", CreatedAt: stampTime}, domain.CommentOpts{}); err != nil {
			t.Fatalf("InsertRecord: %v", err)
		}
		if got := readCommentRows(t, ctx, conn, "cw-repo"); len(got) != 1 || got[0] != wantRow("cmt-repo", "cw-repo", "dave", "repository") {
			t.Errorf("row = %v, want [%s]", got, wantRow("cmt-repo", "cw-repo", "dave", "repository"))
		}
	})

	t.Run("PromoteFromEphemeralInTx carries the wisp comment's stamps", func(t *testing.T) {
		exec("INSERT INTO wisps (id, title, description, design, acceptance_criteria, notes, status, priority, issue_type, ephemeral) VALUES ('cw-promote', 'wisp', '', '', '', '', 'open', 2, 'task', 1)")
		// The wisp comment as the fixed writers leave it: both stamps equal.
		exec("INSERT INTO wisp_comments (id, issue_id, author, text, created_at, updated_at) VALUES ('cmt-wisp', 'cw-promote', 'erin', 'on the wisp', ?, ?)", stamp, stamp)
		if err := issueops.PromoteFromEphemeralInTx(ctx, conn, "cw-promote", "test"); err != nil {
			t.Fatalf("PromoteFromEphemeralInTx: %v", err)
		}
		if got := readCommentRows(t, ctx, conn, "cw-promote"); len(got) != 1 || got[0] != wantRow("cmt-wisp", "cw-promote", "erin", "on the wisp") {
			t.Errorf("promoted row = %v, want [%s]", got, wantRow("cmt-wisp", "cw-promote", "erin", "on the wisp"))
		}
	})

	t.Run("MoveIssuePersistenceInTx carries the stamps across planes", func(t *testing.T) {
		seedIssue("cw-move")
		exec("INSERT INTO comments (id, issue_id, author, text, created_at, updated_at) VALUES ('cmt-move', 'cw-move', 'fay', 'moving', ?, ?)", stamp, stamp)
		current, err := issueops.GetIssueInTx(ctx, conn, "cw-move")
		if err != nil || current == nil {
			t.Fatalf("GetIssueInTx: %v (%v)", err, current)
		}
		if _, err := issueops.MoveIssuePersistenceInTx(ctx, conn, current, types.PersistenceModeEphemeral, "test"); err != nil {
			t.Fatalf("MoveIssuePersistenceInTx(ephemeral): %v", err)
		}
		if got := readCommentRowsFrom(t, ctx, conn, "wisp_comments", "cw-move"); len(got) != 1 || got[0] != wantRow("cmt-move", "cw-move", "fay", "moving") {
			t.Errorf("wisp_comments row after the move = %v, want [%s]", got, wantRow("cmt-move", "cw-move", "fay", "moving"))
		}
		assertScalar(t, ctx, conn, "SELECT COUNT(*) FROM comments WHERE issue_id = 'cw-move'", 0)
	})
}

// readCommentRows returns every comments row of an issue as one line per row:
// id|issue_id|author|text|created_at|updated_at|external_ref.
func readCommentRows(t *testing.T, ctx context.Context, conn *sql.Conn, issueID string) []string {
	t.Helper()
	return readCommentRowsFrom(t, ctx, conn, "comments", issueID)
}

func readCommentRowsFrom(t *testing.T, ctx context.Context, conn *sql.Conn, table, issueID string) []string {
	t.Helper()
	//nolint:gosec // G201: table is one of the two comment tables, chosen by the test.
	rows, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT id, issue_id, author, text, CAST(created_at AS CHAR), CAST(updated_at AS CHAR), COALESCE(external_ref, '') FROM %s WHERE issue_id = ? ORDER BY id", table), issueID)
	if err != nil {
		t.Fatalf("read %s: %v", table, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		cols := make([]string, 7)
		ptrs := make([]any, len(cols))
		for i := range cols {
			ptrs[i] = &cols[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan %s: %v", table, err)
		}
		out = append(out, strings.Join(cols, "|"))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate %s: %v", table, err)
	}
	return out
}

// waitForTheNextSecond sleeps past the next wall-clock second boundary, so a
// CURRENT_TIMESTAMP taken after it differs from one taken before it.
func waitForTheNextSecond() {
	next := time.Now().Truncate(time.Second).Add(time.Second + 50*time.Millisecond)
	time.Sleep(time.Until(next))
}
