package main

// Row is the per-element struct used by every benchmark: 50 string fields
// tagged for JSON, mirroring the 50-cell rows the earlier array-based
// benchmark shape used but exercising the codec's struct-tag path instead.
type Row struct {
	F00 string `json:"f00"`
	F01 string `json:"f01"`
	F02 string `json:"f02"`
	F03 string `json:"f03"`
	F04 string `json:"f04"`
	F05 string `json:"f05"`
	F06 string `json:"f06"`
	F07 string `json:"f07"`
	F08 string `json:"f08"`
	F09 string `json:"f09"`
	F10 string `json:"f10"`
	F11 string `json:"f11"`
	F12 string `json:"f12"`
	F13 string `json:"f13"`
	F14 string `json:"f14"`
	F15 string `json:"f15"`
	F16 string `json:"f16"`
	F17 string `json:"f17"`
	F18 string `json:"f18"`
	F19 string `json:"f19"`
	F20 string `json:"f20"`
	F21 string `json:"f21"`
	F22 string `json:"f22"`
	F23 string `json:"f23"`
	F24 string `json:"f24"`
	F25 string `json:"f25"`
	F26 string `json:"f26"`
	F27 string `json:"f27"`
	F28 string `json:"f28"`
	F29 string `json:"f29"`
	F30 string `json:"f30"`
	F31 string `json:"f31"`
	F32 string `json:"f32"`
	F33 string `json:"f33"`
	F34 string `json:"f34"`
	F35 string `json:"f35"`
	F36 string `json:"f36"`
	F37 string `json:"f37"`
	F38 string `json:"f38"`
	F39 string `json:"f39"`
	F40 string `json:"f40"`
	F41 string `json:"f41"`
	F42 string `json:"f42"`
	F43 string `json:"f43"`
	F44 string `json:"f44"`
	F45 string `json:"f45"`
	F46 string `json:"f46"`
	F47 string `json:"f47"`
	F48 string `json:"f48"`
	F49 string `json:"f49"`
}

// makeRow fills every field with a fresh randStringBytes(rowLen). seed is
// accepted for signature symmetry but unused: all cells are pure random.
func makeRow(rowLen, seed int) Row {
	_ = seed
	return Row{
		F00: randStringBytes(rowLen), F01: randStringBytes(rowLen), F02: randStringBytes(rowLen), F03: randStringBytes(rowLen), F04: randStringBytes(rowLen),
		F05: randStringBytes(rowLen), F06: randStringBytes(rowLen), F07: randStringBytes(rowLen), F08: randStringBytes(rowLen), F09: randStringBytes(rowLen),
		F10: randStringBytes(rowLen), F11: randStringBytes(rowLen), F12: randStringBytes(rowLen), F13: randStringBytes(rowLen), F14: randStringBytes(rowLen),
		F15: randStringBytes(rowLen), F16: randStringBytes(rowLen), F17: randStringBytes(rowLen), F18: randStringBytes(rowLen), F19: randStringBytes(rowLen),
		F20: randStringBytes(rowLen), F21: randStringBytes(rowLen), F22: randStringBytes(rowLen), F23: randStringBytes(rowLen), F24: randStringBytes(rowLen),
		F25: randStringBytes(rowLen), F26: randStringBytes(rowLen), F27: randStringBytes(rowLen), F28: randStringBytes(rowLen), F29: randStringBytes(rowLen),
		F30: randStringBytes(rowLen), F31: randStringBytes(rowLen), F32: randStringBytes(rowLen), F33: randStringBytes(rowLen), F34: randStringBytes(rowLen),
		F35: randStringBytes(rowLen), F36: randStringBytes(rowLen), F37: randStringBytes(rowLen), F38: randStringBytes(rowLen), F39: randStringBytes(rowLen),
		F40: randStringBytes(rowLen), F41: randStringBytes(rowLen), F42: randStringBytes(rowLen), F43: randStringBytes(rowLen), F44: randStringBytes(rowLen),
		F45: randStringBytes(rowLen), F46: randStringBytes(rowLen), F47: randStringBytes(rowLen), F48: randStringBytes(rowLen), F49: randStringBytes(rowLen),
	}
}
