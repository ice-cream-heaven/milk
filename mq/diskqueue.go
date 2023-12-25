package mq

import (
	"bufio"
	"fmt"
	"github.com/ice-cream-heaven/log"
	"github.com/ice-cream-heaven/utils/atexit"
	"github.com/ice-cream-heaven/utils/json"
	"github.com/ice-cream-heaven/utils/osx"
	"github.com/ice-cream-heaven/utils/unit"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"
)

/*
NOTE:
	1. 一个 topic/channel 只有一个 diskqueue
	2. diskqueue 会在 topic/channel 初始化时创建
*/

const (
	writeChanSize = 100
	readChanSize  = 10

	maxMsgSize  = unit.Gb
	maxFileSize = unit.Gb * 2

	syncInterval = time.Second * 2
)

type diskQueue struct {
	sync.RWMutex

	name    string
	dirPath string

	metaFile *os.File

	readFile  *os.File
	writeFile *os.File

	reader *bufio.Reader

	readPos  int64
	writePos int64

	readFileNum  int64
	writeFileNum int64

	writeChan chan *Message
	readChan  chan *Message

	exitChan chan chan struct{}

	depth int64
}

func (p *diskQueue) String() string {
	return p.name
}

func (p *diskQueue) init() (err error) {
	if !osx.IsDir(p.dirPath) {
		err = os.MkdirAll(p.dirPath, 0755)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	err = p.loadMetaData()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	err = p.checkWriteFile()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	err = p.checkReadFile()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}

func (p *diskQueue) loop() {
	var err error
	var message *Message

	syncTicker := time.NewTicker(syncInterval)
	defer syncTicker.Stop()

	for {
		if len(p.readChan) < readChanSize/2 {
			message = p.readOne()
			if message != nil {
				p.readChan <- message
			}
		}

		select {
		case <-syncTicker.C:
			p.sync()

		case message = <-p.writeChan:
			err = p.writeOne(message)
			if err != nil {
				log.Errorf("err:%v", err)
			}

			for {
				select {
				case message = <-p.writeChan:
					err = p.writeOne(message)
					if err != nil {
						log.Errorf(
							"err:%v", err)
					}

				default:
					goto WriteEnd
				}
			}
		WriteEnd:
			p.sync()

		case w := <-p.exitChan:
			for {
				select {
				case message = <-p.writeChan:
					err = p.writeOne(message)
					if err != nil {
						log.Errorf("err:%v", err)
					}

				default:
					goto ExitWriteEnd
				}
			}
		ExitWriteEnd:

			for {
				select {
				case message = <-p.readChan:
					err = p.writeOne(message)
					if err != nil {
						log.Errorf("err:%v", err)
					}

				default:
					goto ExitReadEnd
				}
			}
		ExitReadEnd:
			p.close()
			w <- struct{}{}
		}

		syncTicker.Reset(syncInterval)
	}
}

func (p *diskQueue) writeOne(message *Message) error {
	var err error

	data := message.Encode()
	data = append(data, '\n')
	totalBytes := int64(len(data))

	if totalBytes < 4 || totalBytes > maxMsgSize {
		log.Errorf("invalid message write size (%d)", totalBytes)
		return fmt.Errorf("invalid message write size (%d)", totalBytes)
	}

	if p.writePos > 0 && p.writePos+totalBytes > maxFileSize {
		p.writeFileNum++
		err = p.checkWriteFile()
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	_, err = p.writeFile.Write(data)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	p.writePos += totalBytes

	return err
}

func (p *diskQueue) sync() {
	var err error
	if p.writeFile != nil {
		err = p.writeFile.Sync()
		if err != nil {
			log.Errorf("err:%v", err)
		}
	}

	err = p.saveMetaData()
	if err != nil {
		log.Errorf("err:%v", err)
	}
}

func (p *diskQueue) Sync() {
	var err error
	if p.writeFile != nil {
		err = p.writeFile.Sync()
		if err != nil {
			log.Errorf("err:%v", err)
		}
	}
}

func (p *diskQueue) close() {
	var err error
	if p.writeFile != nil {
		err = p.writeFile.Sync()
		if err != nil {
			log.Errorf("err:%v", err)
		}

		err = p.writeFile.Close()
		if err != nil {
			log.Errorf("err:%v", err)
		}
	}

	if p.readFile != nil {
		err = p.readFile.Sync()
		if err != nil {
			log.Errorf("err:%v", err)
		}

		err = p.readFile.Close()
		if err != nil {
			log.Errorf("err:%v", err)
		}
	}

	err = p.saveMetaData()
	if err != nil {
		log.Errorf("err:%v", err)
	}

	err = p.metaFile.Sync()
	if err != nil {
		log.Errorf("err:%v", err)
	}

	err = p.metaFile.Close()
	if err != nil {
		log.Errorf("err:%v", err)
	}
}

func (p *diskQueue) Close() {
	w := make(chan struct{})
	p.exitChan <- w
	select {
	case <-w:
	case <-time.After(time.Second * 5):
	}
}

func (p *diskQueue) Put(message *Message) {
	p.writeChan <- message
}

func (p *diskQueue) Get() *Message {
	return <-p.readChan
}

func (p *diskQueue) GetWithTimeout(timeout time.Duration) *Message {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case message := <-p.readChan:
		return message
	}
}

func (p *diskQueue) readOne() *Message {
RETRY:
	if p.readFileNum == p.writeFileNum &&
		p.readPos == p.writePos {
		log.Debugf("no message to read")
		return nil
	}

	line, err := p.reader.ReadBytes('\n')
	if err != nil {
		if err == io.EOF {
			if p.readFileNum == p.writeFileNum {
				log.Debugf("no message to read")
				return nil
			}

			p.readFileNum++
			err = p.checkReadFile()
			if err != nil {
				log.Errorf("err:%v", err)
				return nil
			}

			goto RETRY
		}
		log.Errorf("err:%v", err)
		return nil
	}

	p.readPos += int64(len(line))

	message, err := MessageDecode(line[:len(line)-1])
	if err != nil {
		log.Errorf("err:%v", err)
		return nil
	}

	return message
}

func (p *diskQueue) checkReadFile() (err error) {
	if p.readFile != nil {
		fileName := p.readFile.Name()

		err = p.readFile.Sync()
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		err = p.readFile.Close()
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		p.readFile = nil
		p.readPos = 0

		err = os.RemoveAll(fileName)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	p.readFile, err = os.OpenFile(p.fileName(p.readFileNum), os.O_RDONLY, 0600)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	if p.readPos > 0 {
		_, err = p.readFile.Seek(p.readPos, 0)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	p.reader = bufio.NewReader(p.readFile)

	return nil
}

func (p *diskQueue) checkWriteFile() (err error) {
	if p.writeFile != nil {
		err = p.writeFile.Sync()
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		err = p.writeFile.Close()
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		p.writeFile = nil
		p.writePos = 0
	}

	p.writeFile, err = os.OpenFile(p.fileName(p.writeFileNum), os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	if p.writePos > 0 {
		_, err = p.writeFile.Seek(p.writePos, 0)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	return nil
}

type diskQueueMeta struct {
	ReadFileNum  int64 `json:"read_file_num,omitempty"`
	WriteFileNum int64 `json:"write_file_num,omitempty"`

	ReadPos  int64 `json:"read_pos,omitempty"`
	WritePos int64 `json:"write_pos,omitempty"`

	Depth int64 `json:"depth,omitempty"`
}

func (p *diskQueue) metaDataFileName() string {
	return filepath.Join(p.dirPath, "meta.dat")
}

func (p *diskQueue) loadMetaData() (err error) {
	if p.metaFile == nil {
		p.metaFile, err = os.OpenFile(p.metaDataFileName(), os.O_CREATE|os.O_WRONLY|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	_, err = p.metaFile.Seek(0, 0)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	var meta diskQueueMeta
	err = json.NewDecoder(p.metaFile).Decode(&meta)
	if err != nil {
		log.Warnf("load meta data failed, use default")
		log.Errorf("err:%v", err)

	} else {
		p.readFileNum = meta.ReadFileNum
		p.readFileNum = meta.WriteFileNum

		p.readPos = meta.ReadPos
		p.writePos = meta.WritePos

		p.depth = meta.Depth
	}

	return nil
}

func (p *diskQueue) saveMetaData() (err error) {
	_, err = p.metaFile.Seek(0, 0)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	err = json.NewEncoder(p.metaFile).Encode(&diskQueueMeta{
		ReadFileNum:  p.readFileNum,
		WriteFileNum: p.writeFileNum,
		ReadPos:      p.readPos,
		WritePos:     p.writePos,
		Depth:        p.depth,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}

func (p *diskQueue) fileName(fileNum int64) string {
	return filepath.Join(p.dirPath, fmt.Sprintf("%06d.dat", fileNum))
}

func (p *diskQueue) Loop() {
	atexit.Register(func() {
		w := make(chan struct{})
		p.exitChan <- w
		select {
		case <-w:
		case <-time.After(time.Second * 5):
		}
	})
	go p.loop()
}

func newDiskQueue() *diskQueue {
	return &diskQueue{
		writeChan: make(chan *Message, writeChanSize),
		readChan:  make(chan *Message, readChanSize),
		exitChan:  make(chan chan struct{}, 1),
	}
}

func newDiskQueueWithTopic(topic *topic) *diskQueue {
	p := newDiskQueue()
	p.name = topic.String()
	p.dirPath = path.Join(topic.Manager().GetDataDirectory(), topic.Name)

	err := p.init()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil
	}

	return p
}

func newDiskQueueWithChanel(channel *channel) *diskQueue {
	p := newDiskQueue()
	p.name = channel.String()
	p.dirPath = path.Join(channel.Topic().Manager().GetDataDirectory(), channel.Topic().Name, channel.Name)

	err := p.init()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil
	}

	return p
}
