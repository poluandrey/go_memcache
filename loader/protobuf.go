package loader

import (
	"encoding/binary"
	"math"
)

// marshalUserApps encodes UserApps using the protobuf wire format. Keeping the
// tiny encoder local avoids a runtime dependency for this three-field message.
func marshalUserApps(record Record) []byte {
	data := make([]byte, 0, len(record.Apps)*3+18)
	for _, app := range record.Apps {
		data = append(data, 0x08) // field 1, varint
		data = binary.AppendUvarint(data, uint64(app))
	}
	data = append(data, 0x11) // field 2, fixed64
	data = binary.LittleEndian.AppendUint64(data, math.Float64bits(record.Lat))
	data = append(data, 0x19) // field 3, fixed64
	data = binary.LittleEndian.AppendUint64(data, math.Float64bits(record.Lon))
	return data
}
