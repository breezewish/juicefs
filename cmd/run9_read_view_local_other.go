//go:build !linux

package cmd

import "errors"

func openRun9LocalReader(string) (run9ReadFilesystem, func(), error) {
	return nil, nil, errors.New("native Volume read views require Linux")
}
