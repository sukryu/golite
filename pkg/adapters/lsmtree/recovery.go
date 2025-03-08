package lsmtree

import (
	"encoding/binary"
	"io"
	"os"
)

// RecoverFromWAL replays the WAL file to restore the memTable.
func RecoverFromWAL(walPath string, memTable *MemTable) error {
	file, err := os.Open(walPath)
	if err != nil {
		return err
	}
	defer file.Close()

	// 파일 크기가 0이면 바로 복구 종료.
	fi, err := file.Stat()
	if err != nil {
		return err
	}
	if fi.Size() == 0 {
		return nil
	}

	for {
		var opByte [1]byte
		_, err := file.Read(opByte[:])
		if err != nil {
			// 파일 끝이나 예상치 못한 EOF인 경우 종료
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		op := opByte[0]

		var keyLen uint16
		if err := binary.Read(file, binary.BigEndian, &keyLen); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(file, keyBytes); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		key := string(keyBytes)

		var valLen uint16
		if err := binary.Read(file, binary.BigEndian, &valLen); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		valBytes := make([]byte, valLen)
		if _, err := io.ReadFull(file, valBytes); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		value := string(valBytes)

		if op == 0x00 {
			memTable.Insert(key, value)
		} else if op == 0x01 {
			memTable.Delete(key)
		}
	}
	return nil
}
