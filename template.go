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
	em := getEnvMap()
	go func(envMap map[string]string) {
		err = tmpl.Execute(w, envMap)
		if err != nil {
			logger.Printf("applyEnvironmentTemplate: error executing template: %v", err)
			if logger.Verbose() {
				logger.Printf("applyEnvironmentTemplate: env map used for template execution: %v", envMap)
			}
		}
		err = w.Close()
		if err != nil {
			logger.Printf("applyEnvironmentTemplate: error closing writer: %v", err)
		}
	}(em)

	return r, nil
}
