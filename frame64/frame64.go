
package frame64

import (
  "io"
  "strconv"
  "encoding/csv"
  "github.com/gonum/matrix/mat64"
)

type Index struct {
  names []string
}

type ReadOpts struct {

}

var DefaultReadOpts ReadOpts = ReadOpts{}

type DataFrame64 struct {
  Matrix *mat64.Dense
  colNames Index
  rowNames Index
}


func ReadCSV( reader io.Reader, opts ReadOpts ) (*DataFrame64, error) {

  csvReader := csv.NewReader(reader)
  csvReader.Comma = '\t'

  header, err := csvReader.Read()
  if err != nil {
    return &DataFrame64{}, err
  }
  colIndex := NewIndex(header[1:])

  rowNames := make([]string, 0, 1000)
  rows := make([]float64, 0, 10000)
  for {
    row, err := csvReader.Read();
    if err != nil {
      break
    }
    rowNames = append(rowNames, row[0])
    f_row := make([]float64, len(row)-1)
    for i := 1; i < len(row); i++ {
      f, _ := strconv.ParseFloat(row[i], 64)
      f_row[i-1] = f
    }
    rows = append(rows, f_row...)
  }
  rowIndex := NewIndex(rowNames)
  return &DataFrame64{ mat64.NewDense(rowIndex.Size(), colIndex.Size(), rows), colIndex, rowIndex }, nil
}

func (self *DataFrame64) RowNames() []string {
  return self.rowNames.names
}

func (self *DataFrame64) RowNameView(name string) *mat64.Vector {
  i := self.rowNames.Index(name)
  if i == -1 {
    return nil
  }
  return self.Matrix.RowView(i)
}


func NewIndex(names []string) Index {
  return Index{names}
}

func (self *Index) Size() int {
  return len(self.names)
}

func (self *Index) Index(name string) int {
  for i, v := range self.names {
    if v == name {
      return i
    }
  }
  return -1
}
