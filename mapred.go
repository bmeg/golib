
package golib;



func ArrayChan(a []interface{}) chan interface{} {
  out := make(chan interface{}, 100)
  go func() {
    for _, o := range a {
      out <- o
    }
    close(out)
  }()
  return out
}


func StringArrayChan(a []string) chan interface{} {
  out := make(chan interface{}, 100)
  go func() {
    for _, o := range a {
      out <- o
    }
    close(out)
  }()
  return out
}

func Map(input chan interface{}, call func(interface{}) interface{}, workers int ) chan interface{} {
  close_chan := make(chan bool)
  output := make(chan interface{}, workers * 10)
  for i := 0; i < workers; i++ {
    go func() {
      for v := range input {
        output <- call(v)
      }
      close_chan <- true
    }()
  }
  go func() {
    for i := 0; i < workers; i++ {
      <- close_chan
    }
    close(output)
  }()
  return output
}

func FlatMap(input chan interface{}, call func(interface{}, chan interface{}), workers int) chan interface{} {
  close_chan := make(chan bool)
  output := make(chan interface{}, workers * 10)
  for i := 0; i < workers; i++ {
    go func() {
      for v := range input {
        call(v, output)
      }
      close_chan <- true
    }()
  }
  go func() {
    for i := 0; i < workers; i++ {
      <- close_chan
    }
    close(output)
  }()
  return output
}