package integration

import (
	"chukcha/server"
	"chukcha/web"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// InitAndServe checks validity of the supplied arguments and starts
// the web server on the specified port.
func InitAndServe(dirname string, port uint) error {
	filename := filepath.Join(dirname, "write_test")
	fp, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("creating test file %q: %v", filename, err)
	}
	defer fp.Close()
	os.Remove(fp.Name())

	backend, err := server.NewOndisk(dirname)
	if err != nil {
		return fmt.Errorf("initialize on-disk backend: %v", err)
	}

	s := web.NewServer(backend, port)
	log.Printf("Listening connections")
	return s.Serve()
}
