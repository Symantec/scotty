package sysmemory

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
)

var (
	fMemoryPercentage float64
)

var filename string = "/proc/meminfo"

func totalMemoryToUse() (result uint64, err error) {
	if fMemoryPercentage == 0.0 {
		return
	}
	if fMemoryPercentage < 0.0 || fMemoryPercentage > 100.0 {
		err = errors.New("Memory percentage must be between 0 and 100%")
		return
	}
	file, err := os.Open(filename)
	if err != nil {
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var mtype string
		var amount uint64
		mtype, amount, err = processMeminfoLine(scanner.Text())
		if err != nil {
			return
		}
		if mtype == "MemTotal" {
			result = uint64(float64(amount) * fMemoryPercentage / 100.0)
			return
		}
	}
	err = scanner.Err()
	if err == nil {
		err = errors.New("No MemTotal information")
	}
	return
}

func processMeminfoLine(line string) (mtype string, amount uint64, err error) {
	splitLine := strings.SplitN(line, ":", 2)
	if len(splitLine) != 2 {
		return
	}
	mtype = splitLine[0]
	meminfoDataString := strings.TrimSpace(splitLine[1])
	var meminfoData uint64
	var meminfoUnit string
	fmt.Sscanf(meminfoDataString, "%d %s", &meminfoData, &meminfoUnit)
	if meminfoUnit != "kB" {
		err = errors.New(fmt.Sprintf("unknown unit: %s for: %s",
			meminfoUnit, mtype))
		return
	}
	amount = meminfoData * 1024
	return
}

func init() {
	flag.Float64Var(&fMemoryPercentage, "memoryPercentage", 0.0, "Percentage of total memory to use (0.0 - 100.0). 0, the default, means use page_count and bytes_per_page instead.")
}
