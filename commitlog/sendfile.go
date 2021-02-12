package commitlog

import (
	"log"
	"net"
	"os"
	"runtime"
	"syscall"
)

const maxSendfileSize int = 4 << 20

func Sendfile(conn *net.TCPConn, file *os.File, offsetInt int64, size int, chunkSize int) (int, error) {
	offset := &offsetInt
	defer func() {
		runtime.KeepAlive(offset)
	}()
	written := 0
	var remain int = size
	n := chunkSize
	if chunkSize > maxSendfileSize {
		chunkSize = maxSendfileSize
	}
	src := int(file.Fd())
	rawConn, err := conn.SyscallConn()
	rawConn.Write(func(dst uintptr) bool {
		defer func() { log.Println("returned") }()
		for remain > 0 {
			if n > remain {
				n = remain
			}
			var err1 error
			log.Println("params:", n, "offset:", *offset)
			//todo: for bsd and darwin, pass different offset
			n, err1 = syscall.Sendfile(int(dst), src, offset, n)
			log.Println("after:", n, "offset:", *offset)
			if err1 != nil {
				log.Println("sent error:", err1.Error())
			}
			if n > 0 {
				written += n
				remain -= n
			} else if n == 0 && err1 == nil {
				break
			}
			if err1 == syscall.EAGAIN || err1 == syscall.EWOULDBLOCK {

				if n == -1 {
					n = chunkSize
				}
				log.Println("got eagain")
				return false
				// waitpread, gopark
			}
			if err1 != nil {
				// This includes syscall.ENOSYS (no kernel
				// support) and syscall.EINVAL (fd types which
				// don't implement sendfile)
				err = err1
				break
			}
		}
		return true
	})
	log.Println("written", written)

	return written, err

}
