package options

type ResultSetsType uint8

const (
	ResultSetsTypeOrdered = ResultSetsType(iota)
	ResultSetsTypeConcurrent
)
