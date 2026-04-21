//go:build embeddeddolt

package embeddeddolt

import (
	"context"
	"database/sql"

	"github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
)

// CreateAttachment stores attachment metadata in the database.
// Implements storage.AttachmentStore.
func (s *EmbeddedDoltStore) CreateAttachment(ctx context.Context, att *types.Attachment) (*types.Attachment, error) {
	var result *types.Attachment
	err := s.withConn(ctx, true, func(tx *sql.Tx) error {
		var err error
		result, err = issueops.CreateAttachmentInTx(ctx, tx, att)
		return err
	})
	return result, err
}

// GetIssueAttachments retrieves all attachments for an issue.
// Implements storage.AttachmentStore.
func (s *EmbeddedDoltStore) GetIssueAttachments(ctx context.Context, issueID string) ([]*types.Attachment, error) {
	var result []*types.Attachment
	err := s.withConn(ctx, false, func(tx *sql.Tx) error {
		var err error
		result, err = issueops.GetIssueAttachmentsInTx(ctx, tx, issueID)
		return err
	})
	return result, err
}

// GetAttachmentByExternalRef retrieves an attachment by its external_ref.
// Implements storage.AttachmentStore.
func (s *EmbeddedDoltStore) GetAttachmentByExternalRef(ctx context.Context, issueID, externalRef string) (*types.Attachment, error) {
	var result *types.Attachment
	err := s.withConn(ctx, false, func(tx *sql.Tx) error {
		var err error
		result, err = issueops.GetAttachmentByExternalRefInTx(ctx, tx, issueID, externalRef)
		return err
	})
	return result, err
}
