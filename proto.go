

package golib;

import (
  "io"
  "os"
  "fmt"
  "bufio"
  "hash/fnv"
  "path/filepath"
  "encoding/binary"
  "github.com/golang/protobuf/proto"
)

type IDMessage interface {
  proto.Message
  GetId() string
}


func ProtoStreamStore(msgs chan interface{}, fileBase string) {
  out, _ := os.Create(fileBase)
  data := Map(msgs, func(x interface{}) interface{} {
      m := x.(proto.Message)
      b, _ := proto.Marshal(m)
      return b
  }, 8 )
  for ent := range data {
    out.Write(ent.([]byte))
  }  
  out.Close()
}

func ProtoStreamStorePartitions(msgs chan interface{}, fileBase string, partitionCount int) {
  
  out := make([]*os.File, partitionCount)
  for i := 0; i < partitionCount; i += 1 {
    o, _ := os.Create(fmt.Sprintf("%s.%d", fileBase, i))
    out[i] = o
  }
  
  type id_pair struct {
    hash uint32
    msg []byte
  }
  
  data := Map(msgs, func(x interface{}) interface{} {
      m := x.(IDMessage)
      b, _ := proto.Marshal(m)
      h := fnv.New32a()
      h.Write([]byte(m.GetId()))
      return id_pair{ h.Sum32() % uint32(partitionCount), b }
  }, 8 )
  
  offset := make([]byte, 8)
  for ent := range data {
    p := ent.(id_pair)
    binary.PutUvarint(offset, uint64(len(p.msg)))
    out[p.hash].Write(offset)
    out[p.hash].Write(p.msg)
  }
  
  for _, i := range out {
    i.Close()
  }
}

func ProtoReadStream(reader io.Reader, gen func() proto.Message, ) chan interface{} {
  out := make(chan interface{}, 100)
  go func() {
    defer close(out)
    st := bufio.NewReader(reader)
    var err error = nil
    for err != nil {
      var offset uint64
      offset, err = binary.ReadUvarint(st)
      if err == nil {
        data := make([]byte, offset)
        _, err = st.Read(data)
        if err == nil {
          m := gen()
          proto.Unmarshal(data, m)
          out <- m
        }
      }      
    }
  }()
  return out
}

func ProtoStreamReadPartitions(gen func() proto.Message, fileBase string, partitionCount int) chan interface{} {  
  out := make(chan interface{}, 100)
  go func() {
    defer close(out)
    g, _ := filepath.Glob(fmt.Sprintf("%s.*", fileBase))
    for _, path := range g {
      f, _ := os.Open(path)
      for o := range ProtoReadStream(f, gen) {
        out <- o
      }
    }
  }()
  
  return out  
}
