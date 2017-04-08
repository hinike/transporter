// Copyright 2014 The Transporter Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package pipeline provides all adaptoremented functionality to move
// data through transporter.
package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/compose/mejson"
	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/commitlog"
	"github.com/compose/transporter/function"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
	"github.com/compose/transporter/offsetmanager"
	"github.com/compose/transporter/pipe"
)

// A Node is the basic building blocks of transporter pipelines.
// Nodes are constructed in a tree, with the first node broadcasting
// data to each of it's children.
// Node tree's can be constructed as follows:
// 	source := transporter.NewNode("name1", "mongo", adaptor.Config{"uri": "mongodb://localhost/boom", "namespace": "boom.foo", "debug": true})
// 	sink1 := transporter.NewNode("foofile", "file", adaptor.Config{"uri": "stdout://"})
// 	sink2 := transporter.NewNode("foofile2", "file", adaptor.Config{"uri": "stdout://"})
// 	source.Add(sink1)
// 	source.Add(sink2)
//
type Node struct {
	Name       string  `json:"name"`     // the name of this node
	Type       string  `json:"type"`     // the node's type, used to create the adaptorementation
	Children   []*Node `json:"children"` // the nodes are set up as a tree, this is an array of this nodes children
	Parent     *Node   `json:"parent"`   // this node's parent node, if this is nil, this is a 'source' node
	Transforms []*Transform

	nsFilter *regexp.Regexp
	c        client.Client
	reader   client.Reader
	writer   client.Writer
	done     chan struct{}
	wg       sync.WaitGroup
	l        log.Logger
	pipe     *pipe.Pipe
	clog     *commitlog.CommitLog
	om       *offsetmanager.Manager
}

type Transform struct {
	Name     string
	Fn       function.Function
	NsFilter *regexp.Regexp
}

// NewNode creates a new Node struct
func NewNode(name, kind, ns string, a adaptor.Adaptor, parent *Node) (*Node, error) {
	compiledNs, err := regexp.Compile(strings.Trim(ns, "/"))
	if err != nil {
		return nil, err
	}
	n := &Node{
		Name:       name,
		Type:       kind,
		nsFilter:   compiledNs,
		Children:   make([]*Node, 0),
		Transforms: make([]*Transform, 0),
		done:       make(chan struct{}),
	}

	n.c, err = a.Client()
	if err != nil {
		return nil, err
	}

	if parent == nil {
		// TODO: remove path param
		n.pipe = pipe.NewPipe(nil, "")
		n.reader, err = a.Reader()
		if err != nil {
			return nil, err
		}
		n.clog, _ = commitlog.New(
			commitlog.WithPath("/tmp/transporter"),
			commitlog.WithMaxSegmentBytes(1024*1024*1024),
		)
	} else {
		n.Parent = parent
		// TODO: remove path param
		n.pipe = pipe.NewPipe(parent.pipe, "")
		parent.Children = append(parent.Children, n)
		n.clog = parent.clog
		n.om, _ = offsetmanager.New("/tmp/transporter", n.Name)
		n.writer, err = a.Writer(n.done, &n.wg)
		if err != nil {
			return nil, err
		}
	}

	return n, nil
}

// String
func (n *Node) String() string {
	var (
		s, prefix string
		depth     = n.depth()
	)

	prefixformatter := fmt.Sprintf("%%%ds%%-%ds", depth, 18-depth)

	if n.Parent == nil { // root node
		prefix = fmt.Sprintf(prefixformatter, " ", "- Source: ")
	} else {
		prefix = fmt.Sprintf(prefixformatter, " ", "- Sink: ")
	}

	s += fmt.Sprintf("%s %-40s %-15s %-30s", prefix, n.Name, n.Type, n.nsFilter.String())

	for _, child := range n.Children {
		s += "\n" + child.String()
	}
	return s
}

// depth is a measure of how deep into the node tree this node is.  Used to indent the String() stuff
func (n *Node) depth() int {
	if n.Parent == nil {
		return 1
	}

	return 1 + n.Parent.depth()
}

// Path returns a string representation of the names of all the node's parents concatenated with "/"  used in metrics
// eg. for the following tree
// source := transporter.NewNode("name1", "mongo", adaptor.Config{"uri": "mongodb://localhost/boom", "namespace": "boom.foo", "debug": true})
// 	sink1 := transporter.NewNode("foofile", "file", adaptor.Config{"uri": "stdout://"})
// 	source.Add(sink1)
// 'source' will have a Path of 'name1', and 'sink1' will have a path of 'name1/sink1'
func (n *Node) Path() string {
	if n.Parent == nil {
		return n.Name
	}

	return n.Parent.Path() + "/" + n.Name
}

// Start starts the nodes children in a go routine, and then runs either Start() or Listen()
// on the node's adaptor.  Root nodes (nodes with no parent) will run Start()
// and will emit messages to it's children,
// All descendant nodes run Listen() on the adaptor
func (n *Node) Start() error {
	path := n.Path()
	n.l = log.With("name", n.Name).With("type", n.Type).With("path", path)

	for _, child := range n.Children {
		go func(node *Node) {
			node.l = log.With("name", node.Name).With("type", node.Type).With("path", node.Path())
			node.Start()
		}(child)
	}

	if n.Parent == nil {
		nsMap := make(map[string]client.MessageSet)
		nsOffsetMap := make(map[string]uint64)
		// TODO: not entirely sure about this logic check...
		if n.clog.OldestOffset() != n.clog.NewestOffset() {
			var wg sync.WaitGroup
			n.l.With("newestOffset", n.clog.NewestOffset()).
				With("oldestOffset", n.clog.OldestOffset()).
				Infoln("existing messages in commitlog, checking writer offsets...")
			for _, child := range n.Children {
				n.l.With("name", child.Name).Infof("offsetMap: %+v", child.om.OffsetMap())
				if child.om.NewestOffset() < (n.clog.NewestOffset() - 1) {
					wg.Add(1)
					go func() {
						// we subtract 1 from NewestOffset() because we only need to catch up
						// to the last entry in the log
						if err := child.resume(&wg, n.clog.NewestOffset()-1); err != nil {
							n.l.Errorln(err)
						}
					}()
				}

				// compute a map of the oldest offset for every namespace from each child
				for ns, offset := range child.om.OffsetMap() {
					if currentOffset, ok := nsOffsetMap[ns]; !ok || currentOffset > offset {
						nsOffsetMap[ns] = offset
					}
				}
			}
			wg.Wait()
			for ns, offset := range nsOffsetMap {
				r, err := n.clog.NewReader(int64(offset))
				if err != nil {
					return err
				}
				// TODO: move all of the logic below to commitlog.LogEntry
				header := make([]byte, commitlog.LogEntryHeaderLen)
				if _, err = r.Read(header); err != nil {
					return err
				}
				size := commitlog.Encoding.Uint32(header[8:12])
				ts := commitlog.Encoding.Uint64(header[12:20])
				op := commitlog.OpFromBytes(header)
				mode := commitlog.ModeFromBytes(header)
				kvBytes := make([]byte, size)
				if _, err = r.Read(kvBytes); err != nil {
					return err
				}
				keyLen := len(ns) + 8
				d := make(map[string]interface{})
				if err := json.Unmarshal(kvBytes[keyLen:], &d); err != nil {
					return err
				}
				data, err := mejson.Unmarshal(d)
				if err != nil {
					return err
				}
				msg := message.From(op, ns, map[string]interface{}(data))
				nsMap[ns] = client.MessageSet{
					Msg:       msg,
					Timestamp: int64(ts),
					Mode:      mode,
				}
			}
		}
		n.l.Infof("starting with metadata %+v", nsMap)
		return n.start(nsMap)
	}

	return n.listen()
}

func (n *Node) resume(wg *sync.WaitGroup, logOffset int64) error {
	n.l.Infoln("adaptor Resuming...")
	defer func() {
		n.l.Infoln("adaptor Resume complete")
		wg.Done()
	}()

	// TODO: pass in reader as argument so we can keep the commitlog.CommitLog only
	// on the source node
	r, err := n.clog.NewReader(n.om.NewestOffset())
	if err != nil {
		return err
	}
	// TODO: move all of the logic below to commitlog.LogEntry
	for {
		// read each message one at a time by getting the size and then
		// the message and send down the pipe
		header := make([]byte, commitlog.LogEntryHeaderLen)
		if _, err = r.Read(header); err != nil {
			return err
		}
		offset := commitlog.Encoding.Uint64(header[0:8])
		size := commitlog.Encoding.Uint32(header[8:12])
		op := commitlog.OpFromBytes(header)
		kvBytes := make([]byte, size)
		if _, err = r.Read(kvBytes); err != nil {
			return err
		}
		keyLen := commitlog.Encoding.Uint32(kvBytes[0:4])
		ns := string(kvBytes[4 : keyLen+4])
		d := make(map[string]interface{})
		if err := json.Unmarshal(kvBytes[keyLen+8:], &d); err != nil {
			return err
		}
		data, err := mejson.Unmarshal(d)
		if err != nil {
			return err
		}
		// if (offset % 1000) == 0 {
		// 	percentComplete := (float64(offset) / float64(logOffset)) * 100.0
		// 	n.l.With("offset", offset).With("log_offset", logOffset).With("percent_complete", percentComplete).Infoln("still resuming...")
		// }
		n.pipe.In <- message.From(op, ns, map[string]interface{}(data))
		// TODO: remove this when https://github.com/compose/transporter/issues/327
		// is implemented
		n.om.CommitOffset(offsetmanager.Offset{
			Namespace: ns,
			Offset:    offset,
			Timestamp: time.Now().Unix(),
		})
		if offset == uint64(logOffset) {
			n.l.Infoln("offset of message sent down pipe matches logOffset")
			break
		}
	}

	n.l.Infoln("all messages sent down pipeline, waiting for offsets to match...")
	timeout := time.After(60 * time.Second)
	for {
		select {
		case <-timeout:
			n.l.Errorln("resume timeout reached")
			return errors.New("resume timeout reached")
		default:
		}
		n.l.Infoln("checking if offsets match")
		sinkOffset := n.om.NewestOffset()
		if sinkOffset == (logOffset) {
			n.l.Infoln("offsets match!!!")
			return nil
		}
		n.l.With("sink_offset", sinkOffset).With("logOffset", logOffset).Infoln("offsets did not match, checking again in 5 seconds...")
		time.Sleep(5 * time.Second)
	}
}

// Start the adaptor as a source
func (n *Node) start(nsMap map[string]client.MessageSet) error {
	n.l.Infoln("adaptor Starting...")

	s, err := n.c.Connect()
	if err != nil {
		return err
	}
	if closer, ok := s.(client.Closer); ok {
		defer func() {
			n.l.Infoln("closing session...")
			closer.Close()
			n.l.Infoln("session closed...")
		}()
	}
	readFunc := n.reader.Read(nsMap, func(check string) bool { return n.nsFilter.MatchString(check) })
	msgChan, err := readFunc(s, n.done)
	if err != nil {
		return err
	}
	for msg := range msgChan {
		d, _ := mejson.Marshal(msg.Msg.Data().AsMap())
		b, _ := json.Marshal(d)
		offset, err := n.clog.Append(
			commitlog.NewLogFromEntry(
				commitlog.LogEntry{
					Key:       []byte(msg.Msg.Namespace()),
					Mode:      msg.Mode,
					Op:        msg.Msg.OP(),
					Timestamp: uint64(msg.Timestamp),
					Value:     b,
				}))
		if err != nil {
			return err
		}
		// TODO: remove this when https://github.com/compose/transporter/issues/327
		// is implemented
		for _, child := range n.Children {
			child.om.CommitOffset(offsetmanager.Offset{
				Namespace: msg.Msg.Namespace(),
				Offset:    uint64(offset),
				Timestamp: time.Now().Unix(),
			})
		}
		n.l.With("offset", offset).Debugln("attaching offset to message")
		n.pipe.Send(msg.Msg)
	}

	n.l.Infoln("adaptor Start finished...")
	return nil
}

func (n *Node) listen() (err error) {
	n.l.Infoln("adaptor Listening...")
	defer n.l.Infoln("adaptor Listen closed...")

	// TODO: keep n.nsFilter here and remove from pipe.Pipe, we can filter
	// out messages by namespace below in write(), this will allow us to keep
	// the offsetmanager.Manager contained within here and not need to provide
	// it to pipe.Pipe for the cases where we need to ack messages that get
	// filtered out by the namespace filter
	return n.pipe.Listen(n.write, n.nsFilter)
}

func (n *Node) write(msg message.Msg) (message.Msg, error) {
	// TODO: defer func to check if there was an error and if not,
	// call n.om.CommitOffset()
	transformedMsg, err := n.applyTransforms(msg)
	if err != nil {
		return msg, err
	} else if transformedMsg == nil {
		return nil, nil
	}
	sess, err := n.c.Connect()
	if err != nil {
		return msg, err
	}
	defer func() {
		if s, ok := sess.(client.Closer); ok {
			s.Close()
		}
	}()
	returnMsg, err := n.writer.Write(transformedMsg)(sess)
	if err != nil {
		n.pipe.Err <- adaptor.Error{
			Lvl:    adaptor.ERROR,
			Path:   n.Path(),
			Err:    fmt.Sprintf("write message error (%s)", err),
			Record: msg.Data,
		}
	}
	return returnMsg, err
}

func (n *Node) applyTransforms(msg message.Msg) (message.Msg, error) {
	if msg.OP() != ops.Command {
		for _, transform := range n.Transforms {
			if !transform.NsFilter.MatchString(msg.Namespace()) {
				n.l.With("transform", transform.Name).With("ns", msg.Namespace()).Infoln("filtered message")
				continue
			}
			m, err := transform.Fn.Apply(msg)
			if err != nil {
				n.l.Errorf("transform function error, %s", err)
				return nil, err
			} else if m == nil {
				n.l.With("transform", transform.Name).Infoln("returned nil message, skipping")
				return nil, nil
			}
			msg = m
			if msg.OP() == ops.Skip {
				n.l.With("transform", transform.Name).With("op", msg.OP()).Infoln("skipping message")
				return nil, nil
			}
		}
	}
	return msg, nil
}

// Stop this node's adaptor, and sends a stop to each child of this node
func (n *Node) Stop() {
	n.stop()
	for _, node := range n.Children {
		node.Stop()
	}
}

func (n *Node) stop() error {
	n.l.Infoln("adaptor Stopping...")
	n.pipe.Stop()

	close(n.done)
	n.wg.Wait()

	if closer, ok := n.writer.(client.Closer); ok {
		defer func() {
			n.l.Infoln("closing writer...")
			closer.Close()
			n.l.Infoln("writer closed...")
		}()
	}
	if closer, ok := n.c.(client.Closer); ok {
		defer func() {
			n.l.Infoln("closing connection...")
			closer.Close()
			n.l.Infoln("connection closed...")
		}()
	}

	n.l.Infoln("adaptor Stopped")
	return nil
}

// Validate ensures that the node tree conforms to a proper structure.
// Node trees must have at least one source, and one sink.
// dangling transformers are forbidden.  Validate only knows about default adaptors
// in the adaptor package, it can't validate any custom adaptors
func (n *Node) Validate() bool {
	if n.Parent == nil && len(n.Children) == 0 { // the root node should have children
		return false
	}

	for _, child := range n.Children {
		if !child.Validate() {
			return false
		}
	}
	return true
}

// Endpoints recurses down the node tree and accumulates a map associating node name with node type
// this is primarily used with the boot event
func (n *Node) Endpoints() map[string]string {
	m := map[string]string{n.Name: n.Type}
	for _, child := range n.Children {
		childMap := child.Endpoints()
		for k, v := range childMap {
			m[k] = v
		}
	}
	return m
}
