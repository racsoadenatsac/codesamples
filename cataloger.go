package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"sync"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sdk-go/logger"
	"github.com/databricks/databricks-sdk-go/service/catalog"
)

type CustomCredentials struct{}

func (c *CustomCredentials) Name() string {
	return "custom"
}

func (c *CustomCredentials) Configure(
	ctx context.Context, cfg *config.Config,
) (func(*http.Request) error, error) {
	return func(r *http.Request) error {
		token := ""
		r.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
		return nil
	}, nil
}

func main() {

	//var wg sync.WaitGroup

	ctx := context.Background()
	w, err := databricks.NewWorkspaceClient(&databricks.Config{
		Host:        "",
		Credentials: &CustomCredentials{},
	})
	if err != nil {
		panic(err)
	}

	//Update_Owner_Table(ctx, w)
	//Update_Permissions_Table(ctx, w)

	Update_Permissions_Schema(ctx, w)
	//Remove_Permissions_Schema(ctx, w)

	/*
		start := time.Now()
		query := ""
		tableNames := extractTableNames(query, true)
		for _, tableName := range tableNames {
			fmt.Println(tableName)
			Update_Permissions_Table(ctx, w, "Operaciones", tableName, []catalog.Privilege{catalog.PrivilegeSelect}, &wg)
		}
		wg.Wait()
		elapsed := time.Since(start)
		fmt.Printf("Execution time with goroutines: %s\n", elapsed)
	*/
}

func Update_Owner_Table(ctx context.Context, w *databricks.WorkspaceClient) {

	summaries, err := w.Tables.ListSummariesAll(ctx, catalog.ListSummariesRequest{
		CatalogName:       "",
		SchemaNamePattern: "",
	})
	if err != nil {
		panic(err)
	}

	for _, summary := range summaries {
		w.Tables.Update(ctx, catalog.UpdateTableRequest{
			FullName: summary.FullName,
			Owner:    "Administrators",
		})
		logger.Infof(ctx, "Updated: %v\n", summary.FullName)
	}

}

func Update_Permissions_Table(ctx context.Context, w *databricks.WorkspaceClient, principal string, tableFullname string, permissions []catalog.Privilege, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		x, err := w.Grants.Update(ctx, catalog.UpdatePermissions{
			FullName:      tableFullname,
			SecurableType: catalog.SecurableTypeTable,

			Changes: []catalog.PermissionsChange{catalog.PermissionsChange{
				Add:       permissions,
				Principal: principal,
			}},
		})
		if err != nil {
			logger.Errorf(ctx, "Error updating permissions: %v", err)
			return
		}
		logger.Infof(ctx, "Updated %v", x)
	}()
}

func Update_Permissions_Schema(ctx context.Context, w *databricks.WorkspaceClient) {

	x, err := w.Grants.Update(ctx, catalog.UpdatePermissions{
		FullName:      "quickstart_catalog.operaciones",
		SecurableType: catalog.SecurableTypeSchema,
		Changes: []catalog.PermissionsChange{catalog.PermissionsChange{
			Add:       []catalog.Privilege{catalog.PrivilegeAllPrivileges},
			Principal: "OperacionesDev",
		}},
	})
	if err != nil {
		panic(err)
	}
	logger.Infof(ctx, "Updated %v", x)
}

func Update_Permissions_Catalog(ctx context.Context, w *databricks.WorkspaceClient) {

	x, err := w.Grants.Update(ctx, catalog.UpdatePermissions{
		FullName:      "quickstart_catalog.main.datamart_customer_api_tracking",
		SecurableType: catalog.SecurableType(catalog.CatalogTypeManagedCatalog),
		Changes: []catalog.PermissionsChange{catalog.PermissionsChange{
			Add:       []catalog.Privilege{catalog.PrivilegeAllPrivileges},
			Principal: "",
		}},
	})
	if err != nil {
		panic(err)
	}
	logger.Infof(ctx, "Updated %v", x)
}

func Remove_Permissions_Schema(ctx context.Context, w *databricks.WorkspaceClient) {
	x, err := w.Grants.Update(ctx, catalog.UpdatePermissions{
		FullName:      "quickstart_catalog.operaciones",
		SecurableType: catalog.SecurableTypeSchema,
		Changes: []catalog.PermissionsChange{catalog.PermissionsChange{
			Remove:    []catalog.Privilege{catalog.PrivilegeAllPrivileges},
			Principal: "account users",
		}},
	})
	if err != nil {
		panic(err)
	}
	logger.Infof(ctx, "Updated %v", x)

}

func extractTableNames(query string, csvFile bool) []string {

	tableNames := make(map[string]bool)
	result := make([]string, 0, len(tableNames))

	if csvFile {
		file, err := os.Open("tables.csv")
		if err != nil {
			panic(err)
		}
		defer file.Close()

		reader := csv.NewReader(file)

		// Read the first line and discard it
		_, err = reader.Read()
		if err != nil {
			panic(err)
		}
		// Read the remaining lines
		records, err := reader.ReadAll()
		if err != nil {
			panic(err)
		}
		for _, record := range records {
			result = append(result, record[0]+"."+record[1]+"."+record[2])
		}
	} else {

		regex := regexp.MustCompile(`(?i)FROM\s+(\w+\.\w+\.\w+)`)

		matches := regex.FindAllStringSubmatch(query, -1)
		for _, match := range matches {
			if len(match) > 1 {
				tableName := match[1]
				tableNames[tableName] = true
			}
		}
		for tableName := range tableNames {
			result = append(result, tableName)
		}
	}
	return result
}
