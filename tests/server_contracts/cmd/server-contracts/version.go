package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/draft/Ydb_Maintenance_V1"
	"github.com/ydb-platform/ydb-go-genproto/draft/protos/Ydb_Maintenance"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
)

type ydbRuntimeIdentity struct {
	serverVersions   []string
	serverVersionAPI string
	serverVersionErr error
	image            dockerImageMetadata
	imageTag         string
}

func ydbServerVersions(
	ctx context.Context,
	connectionString string,
	monitoringURL string,
) ([]string, string, error) {
	versions, maintenanceErr := maintenanceAPIServerVersions(ctx, connectionString)
	if maintenanceErr == nil {
		return versions, "Maintenance API", nil
	}
	versions, viewerErr := viewerAPIServerVersions(ctx, monitoringURL)
	if viewerErr == nil {
		return versions, "Viewer API", nil
	}

	return nil, "YDB API", errors.Join(maintenanceErr, viewerErr)
}

func maintenanceAPIServerVersions(ctx context.Context, connectionString string) ([]string, error) {
	versionCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	driver, err := ydb.Open(versionCtx, connectionString, ydb.WithAnonymousCredentials())
	if err != nil {
		return nil, fmt.Errorf("open YDB driver: %w", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = driver.Close(closeCtx)
	}()

	response, err := Ydb_Maintenance_V1.NewMaintenanceServiceClient(ydb.GRPCConn(driver)).ListClusterNodes(
		versionCtx,
		&Ydb_Maintenance.ListClusterNodesRequest{
			OperationParams: &Ydb_Operations.OperationParams{
				OperationMode: Ydb_Operations.OperationParams_SYNC,
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("ListClusterNodes: %w", err)
	}

	return nodeVersions(response.GetOperation())
}

func nodeVersions(operation *Ydb_Operations.Operation) ([]string, error) {
	if operation == nil {
		return nil, errors.New("ListClusterNodes returned no operation")
	}
	if !operation.GetReady() {
		return nil, errors.New("ListClusterNodes returned an unfinished synchronous operation")
	}
	if operation.GetStatus() != Ydb.StatusIds_SUCCESS {
		return nil, fmt.Errorf(
			"ListClusterNodes returned %s: %v",
			operation.GetStatus(),
			operation.GetIssues(),
		)
	}

	var result Ydb_Maintenance.ListClusterNodesResult
	if operation.GetResult() == nil {
		return nil, errors.New("ListClusterNodes returned no result")
	}
	if err := operation.GetResult().UnmarshalTo(&result); err != nil {
		return nil, fmt.Errorf("decode ListClusterNodes result: %w", err)
	}

	unique := make(map[string]struct{})
	for _, node := range result.GetNodes() {
		if version := strings.TrimSpace(node.GetVersion()); version != "" {
			unique[version] = struct{}{}
		}
	}
	versions := make([]string, 0, len(unique))
	for version := range unique {
		versions = append(versions, version)
	}
	sort.Strings(versions)
	if len(versions) == 0 {
		return nil, errors.New("ListClusterNodes returned no node version")
	}

	return versions, nil
}

func viewerAPIServerVersions(ctx context.Context, monitoringURL string) ([]string, error) {
	versionCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	request, err := http.NewRequestWithContext(
		versionCtx,
		http.MethodGet,
		strings.TrimRight(monitoringURL, "/")+"/viewer/json/sysinfo?enums=1",
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("create Viewer API request: %w", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("viewer API sysinfo: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("viewer API sysinfo returned HTTP %s", response.Status)
	}

	var document any
	decoder := json.NewDecoder(io.LimitReader(response.Body, 4<<20))
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("decode Viewer API sysinfo: %w", err)
	}
	unique := make(map[string]struct{})
	collectJSONVersions(document, unique)
	versions := make([]string, 0, len(unique))
	for version := range unique {
		versions = append(versions, version)
	}
	sort.Strings(versions)
	if len(versions) == 0 {
		return nil, errors.New("viewer API sysinfo returned no Version field")
	}

	return versions, nil
}

func collectJSONVersions(value any, versions map[string]struct{}) {
	switch value := value.(type) {
	case map[string]any:
		for key, nested := range value {
			if strings.EqualFold(key, "version") {
				if version, ok := nested.(string); ok && strings.TrimSpace(version) != "" {
					versions[strings.TrimSpace(version)] = struct{}{}
				}
			}
			collectJSONVersions(nested, versions)
		}
	case []any:
		for _, nested := range value {
			collectJSONVersions(nested, versions)
		}
	}
}

func printYDBRuntimeIdentity(out io.Writer, identity ydbRuntimeIdentity) {
	if identity.serverVersionErr != nil {
		fmt.Fprintf(out, "YDB server version (%s): unavailable (%v)\n", identity.serverVersionAPI, identity.serverVersionErr)
	} else {
		fmt.Fprintf(
			out,
			"YDB server version (%s): %s\n",
			identity.serverVersionAPI,
			strings.Join(identity.serverVersions, ", "),
		)
	}

	imageReference := imageRepository + ":" + identity.imageTag
	if identity.image.digest != "" {
		imageReference += "@" + identity.image.digest
	}
	fmt.Fprintf(out, "Docker image: %s\n", imageReference)
	if identity.image.digest == "" && identity.image.imageID != "" {
		fmt.Fprintf(out, "Docker image ID: %s\n", identity.image.imageID)
	}
	if identity.image.revision != "" {
		fmt.Fprintf(out, "YDB source revision (image): %s\n", identity.image.revision)
	}
	if identity.image.versionLabel != "" {
		fmt.Fprintf(out, "OCI version label: %s\n", identity.image.versionLabel)
	}
}

func waitForYDB(ctx context.Context, connectionString string) error {
	readyCtx, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()

	var lastErr error
	for {
		attemptCtx, attemptCancel := context.WithTimeout(readyCtx, 5*time.Second)
		driver, err := ydb.Open(attemptCtx, connectionString, ydb.WithAnonymousCredentials())
		if err == nil {
			_, err = driver.Scheme().ListDirectory(attemptCtx, "/local")
			closeErr := driver.Close(attemptCtx)
			if err == nil {
				err = closeErr
			}
		}
		attemptCancel()
		if err == nil {
			return nil
		}
		lastErr = err

		select {
		case <-readyCtx.Done():
			return fmt.Errorf("YDB did not become ready within %s: %w", readyTimeout, lastErr)
		case <-time.After(500 * time.Millisecond):
		}
	}
}
