package ranges

import (
	"io/ioutil"
)

func InitRanges(dir string) error {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {

		}
	}

	return nil
}
