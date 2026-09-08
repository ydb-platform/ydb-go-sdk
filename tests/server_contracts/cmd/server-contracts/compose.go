package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	imageRepository = "ydbplatform/local-ydb"
	readyTimeout    = 2 * time.Minute
)

type composeStack struct {
	file           string
	project        string
	grpcPort       int
	monitoringPort int
	out            io.Writer
	errOut         io.Writer
	downOnce       sync.Once
	downErr        error
}

type dockerImageMetadata struct {
	versionLabel string
	revision     string
	digest       string
	imageID      string
}

func checkCompose(ctx context.Context) error {
	command := exec.CommandContext(ctx, "docker", "compose", "version")
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("docker compose v2 is required: %w: %s", err, strings.TrimSpace(string(output)))
	}

	return nil
}

func availablePort(ctx context.Context) (int, error) {
	var config net.ListenConfig
	listener, err := config.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("allocate local gRPC port: %w", err)
	}
	defer listener.Close()

	address, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("unexpected listener address %T", listener.Addr())
	}

	return address.Port, nil
}

func newComposeStack(ctx context.Context, version string, out, errOut io.Writer) (*composeStack, error) {
	port, err := availablePort(ctx)
	if err != nil {
		return nil, err
	}
	monitoringPort := port
	for monitoringPort == port {
		monitoringPort, err = availablePort(ctx)
		if err != nil {
			return nil, err
		}
	}
	directory, err := os.MkdirTemp("", "ydb-server-research-")
	if err != nil {
		return nil, fmt.Errorf("create temporary Compose directory: %w", err)
	}
	stack := &composeStack{
		file:     filepath.Join(directory, "compose.yaml"),
		project:  fmt.Sprintf("ydb-research-%d-%d", os.Getpid(), time.Now().UnixNano()),
		grpcPort: port, monitoringPort: monitoringPort, out: out, errOut: errOut,
	}
	if err := os.WriteFile(stack.file, []byte(composeYAML(version, port, monitoringPort)), 0o600); err != nil {
		return nil, errors.Join(fmt.Errorf("write temporary Compose file: %w", err), os.RemoveAll(directory))
	}

	return stack, nil
}

func (s *composeStack) cleanup() error {
	fmt.Fprintln(s.out, "\nStopping YDB with Docker Compose...")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := s.Down(ctx); err != nil {
		// Keep the exact project configuration available for a manual cleanup retry.
		return fmt.Errorf("%w; Compose configuration retained at %s (project %s)", err, s.file, s.project)
	}

	return os.RemoveAll(filepath.Dir(s.file))
}

func composeYAML(version string, port, monitoringPort int) string {
	return fmt.Sprintf(`services:
  ydb:
    image: %s:%s
    platform: linux/amd64
    hostname: localhost
    environment:
      GRPC_PORT: "%d"
      MON_PORT: "8765"
      YDB_GRPC_ENABLE_TLS: "0"
      YDB_USE_IN_MEMORY_PDISKS: "true"
      YDB_DEFAULT_LOG_LEVEL: "NOTICE"
    ports:
      - "127.0.0.1:%d:%d"
      - "127.0.0.1:%d:8765"
`, imageRepository, version, port, port, port, monitoringPort)
}

func (s *composeStack) command(ctx context.Context, arguments ...string) *exec.Cmd {
	base := []string{"compose", "--project-name", s.project, "--file", s.file}

	return exec.CommandContext(ctx, "docker", append(base, arguments...)...)
}

func (s *composeStack) Up(ctx context.Context) error {
	command := s.command(ctx, "up", "--detach", "--wait", "--wait-timeout", strconv.Itoa(int(readyTimeout.Seconds())))
	command.Stdout = s.out
	command.Stderr = s.errOut
	if err := command.Run(); err != nil {
		return fmt.Errorf("docker compose up: %w", err)
	}

	return nil
}

func (s *composeStack) Down(ctx context.Context) error {
	s.downOnce.Do(func() {
		command := s.command(ctx, "down", "--volumes", "--remove-orphans")
		command.Stdout = s.out
		command.Stderr = s.errOut
		if err := command.Run(); err != nil {
			s.downErr = fmt.Errorf("docker compose down: %w", err)
		}
	})

	return s.downErr
}

func (s *composeStack) Logs(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	command := s.command(ctx, "logs", "--no-color", "--tail", "100", "ydb")
	command.Stdout = s.errOut
	command.Stderr = s.errOut

	return command.Run()
}

func (s *composeStack) ImageMetadata(ctx context.Context) dockerImageMetadata {
	containerCommand := s.command(ctx, "ps", "--quiet", "ydb")
	containerID, err := containerCommand.Output()
	if err != nil || strings.TrimSpace(string(containerID)) == "" {
		return dockerImageMetadata{}
	}
	inspectCommand := exec.CommandContext(
		ctx,
		"docker",
		"inspect",
		"--format",
		strings.Join([]string{
			`{{.Image}}`,
			`{{index .Config.Labels "org.opencontainers.image.version"}}`,
			`{{index .Config.Labels "org.opencontainers.image.revision"}}`,
			`{{index .Config.Labels "ydb.revision"}}`,
		}, "\n"),
		strings.TrimSpace(string(containerID)),
	)
	output, err := inspectCommand.Output()
	if err != nil {
		return dockerImageMetadata{}
	}
	lines := strings.Split(strings.TrimSuffix(string(output), "\n"), "\n")
	for len(lines) < 4 {
		lines = append(lines, "")
	}
	metadata := dockerImageMetadata{
		imageID:      dockerMetadataValue(lines[0]),
		versionLabel: dockerMetadataValue(lines[1]),
		revision:     dockerMetadataValue(lines[2]),
	}
	if metadata.revision == "" {
		metadata.revision = dockerMetadataValue(lines[3])
	}
	if metadata.imageID == "" {
		return metadata
	}

	metadata.digest = imageDigest(ctx, metadata.imageID)

	return metadata
}

func imageDigest(ctx context.Context, imageID string) string {
	imageInspectCommand := exec.CommandContext(
		ctx,
		"docker",
		"image",
		"inspect",
		"--format",
		`{{json .RepoDigests}}`,
		imageID,
	)
	repoDigests, err := imageInspectCommand.Output()
	if err != nil {
		return ""
	}
	var references []string
	if err := json.Unmarshal(repoDigests, &references); err != nil {
		return ""
	}
	var selected string
	for _, reference := range references {
		repository, digest, found := strings.Cut(reference, "@")
		if found && (selected == "" || repository == imageRepository) {
			selected = digest
		}
		if repository == imageRepository {
			break
		}
	}

	return selected
}

func dockerMetadataValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "<no value>" {
		return ""
	}

	return value
}
