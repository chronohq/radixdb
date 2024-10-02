package radixdb

const (
	// magicByte is the first byte of a RadixDB file, used to identify the format
	// and detect pontential corruption. It is represented by 'R' for RadixDB.
	magicByte = 0x52

	// fileExt is the standard file extension for RadixDB files.
	fileExt = ".rdx"
)
