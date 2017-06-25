
package normalize

import (
  "sort"
)


func Quantile(x []float64) []float64 {
  tmp := make(map[float64]int, len(x))
  for _, v := range x {
    tmp[v] = 0
  }
  s := make([]float64, 0, len(x))
  for i := range tmp {
    s = append(s, i)
  }
  sort.Float64s(s)
  for i, v := range s {
    tmp[v] = i
  }
  out := make([]float64, len(x))
  for i, v := range x {
    out[i] = float64(tmp[v]) / float64(len(tmp))
  }
  return out
}
