

package golib;

import (
  "os"
  "fmt"
  "hash/fnv"
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
  
  for ent := range data {
    p := ent.(id_pair)
    out[p.hash].Write(p.msg)
  }
  
  for _, i := range out {
    i.Close()
  }
}

func ProtoStreamReadPartitions(fileBase string, partitionCount int) chan interface{} {
  
  
  
}
