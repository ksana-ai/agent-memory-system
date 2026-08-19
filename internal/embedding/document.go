package embedding

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"math"
	"strconv"
	"strings"

	"github.com/kai443/go-agent-memory-system/internal/domain"
)

const (
	MemoryCardDocumentVersion = "memory-card-document-v1"
	RawQueryVersion           = "raw-query-v1"

	// ProbeTextV1 is a behavioral probe input. Its resulting vector hash can
	// detect a changed served model/configuration, but is not a model-weights
	// hash and must never be described as one.
	ProbeTextV1       = "go-agent-memory-system embedding probe v1 | 用户偏好 semantic memory"
	ProbeTextV1SHA256 = "7dfb094cf18058f677eac463df586c1ab519bcca9a348973ea5ab40038be94ae"
)

var ErrInvalidSpace = errors.New("invalid embedding space identity")

// MemoryCardDocumentV1 produces the stable semantic document embedded for a
// reviewed memory. Operational identifiers, scope, lifecycle state, source
// IDs, and timestamps are deliberately excluded: they are filters/provenance,
// not semantic retrieval content.
//
// Values use Go's deterministic quoted-string format so embedded newlines and
// delimiter characters cannot alter the field structure. The trailing newline
// is part of the versioned format.
func MemoryCardDocumentV1(card domain.MemoryCard) string {
	var document strings.Builder
	writeDocumentField(&document, "document_version", MemoryCardDocumentVersion)
	writeDocumentField(&document, "kind", string(card.Kind))
	writeDocumentField(&document, "category", card.Category)
	writeDocumentField(&document, "key", card.Key)
	writeDocumentField(&document, "value", card.Value)
	writeDocumentField(&document, "person", card.Person)
	writeDocumentField(&document, "relationship", card.Relationship)
	writeDocumentField(&document, "backstory", card.Backstory)
	return document.String()
}

// DocumentSHA256 returns a lowercase hexadecimal SHA-256 of the exact
// versioned document bytes.
func DocumentSHA256(document string) string {
	digest := sha256.Sum256([]byte(document))
	return hex.EncodeToString(digest[:])
}

// MemoryCardDocumentV1SHA256 fingerprints exactly MemoryCardDocumentV1(card).
func MemoryCardDocumentV1SHA256(card domain.MemoryCard) string {
	return DocumentSHA256(MemoryCardDocumentV1(card))
}

// VectorSHA256 fingerprints the exact IEEE 754 float32 bit patterns in fixed
// big-endian byte order. This makes hashes independent of machine byte order.
// It is a vector/probe fingerprint, never evidence of model weight identity.
func VectorSHA256(vector []float32) string {
	digest := sha256.New()
	var encoded [4]byte
	for _, value := range vector {
		binary.BigEndian.PutUint32(encoded[:], math.Float32bits(value))
		_, _ = digest.Write(encoded[:])
	}
	return hex.EncodeToString(digest.Sum(nil))
}

// SpaceID returns a stable identity for one mutually compatible vector space.
// modelFingerprint is expected to be the fixed probe vector hash; it is not a
// model weight hash. Strings are encoded with lengths in a fixed field order,
// preventing concatenation collisions.
func SpaceID(provider, model string, dimension int, documentVersion, queryVersion, modelFingerprint string) (string, error) {
	fields := []struct {
		name  string
		value string
	}{
		{name: "provider", value: provider},
		{name: "model", value: model},
		{name: "document version", value: documentVersion},
		{name: "query version", value: queryVersion},
		{name: "model fingerprint", value: modelFingerprint},
	}
	for _, field := range fields {
		if strings.TrimSpace(field.value) == "" {
			return "", fmt.Errorf("%w: %s is required", ErrInvalidSpace, field.name)
		}
	}
	if dimension < 1 {
		return "", fmt.Errorf("%w: dimension must be positive", ErrInvalidSpace)
	}

	digest := sha256.New()
	writeLengthPrefixed(digest, "embedding-space-v1")
	writeLengthPrefixed(digest, provider)
	writeLengthPrefixed(digest, model)
	var encodedDimension [8]byte
	binary.BigEndian.PutUint64(encodedDimension[:], uint64(dimension))
	_, _ = digest.Write(encodedDimension[:])
	writeLengthPrefixed(digest, documentVersion)
	writeLengthPrefixed(digest, queryVersion)
	writeLengthPrefixed(digest, modelFingerprint)
	return "space_v1_" + hex.EncodeToString(digest.Sum(nil)), nil
}

func writeDocumentField(document *strings.Builder, name, value string) {
	document.WriteString(name)
	document.WriteByte('=')
	document.WriteString(strconv.Quote(value))
	document.WriteByte('\n')
}

func writeLengthPrefixed(destination hash.Hash, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = destination.Write(length[:])
	_, _ = destination.Write([]byte(value))
}
