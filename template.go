package migrate

import (
	"fmt"
	"io"
	"text/template"
)

var envMapCache map[string]string

func applyEnvironmentTemplate(body io.ReadCloser, logger Logger) (io.ReadCloser, error) {
	bodyBytes, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("reading body: %w", err)
	}
	defer func() {
		err = body.Close()
		if err != nil {
			logger.Printf("applyEnvironmentTemplate: error closing body: %v", err)
		}
	}()

	tmpl, err := template.New("migration").Parse(string(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("parsing template: %w", err)
	}

	r, w := io.Pipe()
	// we are using env map in gorutines without passing them as goroutine function arguments as we dont need seperate copies of the map in each go routine
	// all operations in the goroutines involving the map are read only in nature
	envMap := getEnvMap()
	go func() {
		err = tmpl.Execute(w, envMap)
		if err != nil {
			if logger != nil {
				logger.Printf("applyEnvironmentTemplate: error executing template: %v", err)
				if logger.Verbose() {
					logger.Printf("applyEnvironmentTemplate: env map used for template execution: %v", envMap)
				}
			}
		}
		err = w.Close()
		if err != nil {
			if logger != nil {
				logger.Printf("applyEnvironmentTemplate: error closing writer: %v", err)
			}
		}
	}()

	return r, nil
}
