package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"synch/internal/eventlog"
)

type Request struct {
	Caller, ID, Method, Route string
	PathParams                map[string]string
	Body                      []byte
	Command                   string
}
type Response struct {
	ReceiptID, PayloadDigest, Status, ContentType string
	HTTPStatus                                    int
	Body                                          []byte
	Replayed                                      bool
	Record                                        []byte
	decisionID, decisionThread                    string
}
type Conflict struct {
	StoredPayloadDigest string `json:"stored_payload_digest"`
	ReceiptID           string `json:"receipt_id"`
}

// Error returns the problem code for a reused request ID with a different payload.
func (c *Conflict) Error() string { return "request_id_conflict" }

type apply func(context.Context, *eventlog.Transaction, Response, map[string]any) (Response, error)

var requestID = regexp.MustCompile("^[A-Za-z0-9._:-]{1,128}$")

// execute performs pre-receipt admission and serializes lookup, replay, and first application.
func execute(ctx context.Context, s *eventlog.Store, r Request, apply apply) (Response, error) {
	if r.Caller == "" {
		return Response{}, errors.New("unauthorized")
	}
	if r.ID == "" {
		return Response{}, errors.New("request_id_required")
	}
	if !requestID.MatchString(r.ID) {
		return Response{}, errors.New("request_id_invalid")
	}
	body, err := Admit(r.Route, r.Body)
	if err != nil {
		return Response{}, err
	}
	digest, err := digestBody(r.Method, r.Route, r.PathParams, body)
	if err != nil {
		return Response{}, err
	}
	var response Response
	err = s.Write(ctx, func(tx *eventlog.Transaction) error {
		err := tx.QueryRowContext(ctx, `SELECT receipt_id,payload_digest,status,http_status,content_type,body,record FROM receipts WHERE caller_identity=? AND request_id=?`, r.Caller, r.ID).Scan(&response.ReceiptID, &response.PayloadDigest, &response.Status, &response.HTTPStatus, &response.ContentType, &response.Body, &response.Record)
		if err == nil {
			if response.PayloadDigest != digest {
				return &Conflict{response.PayloadDigest, response.ReceiptID}
			}
			response.Replayed = true
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("lookup receipt: %w", err)
		}
		seed := Response{ReceiptID: eventlog.NewID(), PayloadDigest: digest}
		response, err = apply(ctx, tx, seed, body)
		if err != nil {
			return err
		}
		response.ReceiptID = seed.ReceiptID
		response.PayloadDigest = digest
		response.Replayed = false
		if err := validateResponse(response); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO receipts(caller_identity,request_id,receipt_id,payload_digest,status,http_status,content_type,body,record) VALUES(?,?,?,?,?,?,?,?,?)`, r.Caller, r.ID, response.ReceiptID, digest, response.Status, response.HTTPStatus, response.ContentType, response.Body, response.Record)
		if err == nil && response.decisionID != "" {
			_, err = tx.ExecContext(ctx, "INSERT INTO decision_receipts(thread_id,decision_id,receipt_id) VALUES(?,?,?)", response.decisionThread, response.decisionID, response.ReceiptID)
		}
		return err
	})
	if err != nil {
		return Response{}, err
	}
	return response, nil
}

// validateResponse checks that only contract-defined acceptance or rejection outcomes are stored.
func validateResponse(r Response) error {
	var body struct {
		ReceiptID string `json:"receipt_id"`
		Code      string `json:"code"`
	}
	if json.Unmarshal(r.Body, &body) != nil || body.ReceiptID != r.ReceiptID {
		return errors.New("invalid receipt response body")
	}
	if r.Status == "accepted" && (r.HTTPStatus == 201 || r.HTTPStatus == 202) && r.ContentType == "application/json" {
		return nil
	}
	if status, ok := rejectionStatus[body.Code]; ok && r.Status == "rejected" && r.HTTPStatus == status && r.ContentType == "application/problem+json" {
		return nil
	}
	return errors.New("invalid deterministic receipt outcome")
}
