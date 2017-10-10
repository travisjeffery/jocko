package jocko

import (
	"net"
	"strconv"
)

func SplitHostPort(addr string) (string, int, error) {
	addr, strPort, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}

	port, err := strconv.Atoi(strPort)
	if err != nil {
		return "", 0, err
	}

	return addr, port, nil
}
