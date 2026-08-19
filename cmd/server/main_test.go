package main

import (
	"os"
	"strings"
	"testing"
)

func TestOpenAPIHealthPhaseMatchesServer(t *testing.T) {
	contract, err := os.ReadFile("../../api/openapi.yaml")
	if err != nil {
		t.Fatalf("read OpenAPI contract: %v", err)
	}
	want := "phase: {type: string, const: " + serverPhase + "}"
	if !strings.Contains(string(contract), want) {
		t.Fatalf("OpenAPI health phase does not match server phase %q", serverPhase)
	}
}
