package value

import "fmt"

func CastTo(v Value, dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		return castToString(v, vv)
	case *[]byte:
		return castToBytes(v, vv)
	case *uint64:
		return catToUint64(v, vv)
	case *int64:
		return castToInt64(v, vv)
	case *uint32:
		return castToUint32(v, vv)
	case *int32:
		return castToInt32(v, vv)
	case *uint16:
		return castToUint16(v, vv)
	case *int16:
		return castToInt16(v, vv)
	case *uint8:
		return castToUint8(v, vv)
	case *int8:
		return castToInt8(v, vv)
	case *float64:
		return castToFloat64(v, vv)
	case *float32:
		return castToFloat32(v, vv)
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), vv)
	}
}

func castToString(v Value, dst *string) error {
	switch vv := v.(type) {
	case *utf8Value:
		*dst = vv.value
		return nil
	case stringValue:
		*dst = string(vv)
		return nil
	}
	return fmt.Errorf("cannot cast YDB type '%s' to 'string' destination", v.Type().String())
}

func castToBytes(v Value, dst *[]byte) error {
	switch vv := v.(type) {
	case *utf8Value:
		*dst = []byte(vv.value)
		return nil
	case stringValue:
		*dst = vv
		return nil
	}
	return fmt.Errorf("cannot cast YDB type '%s' to '[]byte' destination", v.Type().String())
}

func catToUint64(v Value, dst *uint64) error {
	switch vv := v.(type) {
	case uint64Value:
		*dst = uint64(vv)
		return nil
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to 'uint64' destination", v.Type().String())
	}
}

func castToInt64(v Value, dst *int64) error {
	switch vv := v.(type) {
	case int64Value:
		*dst = int64(vv)
		return nil
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to 'int64' destination", v.Type().String())
	}
}

func castToUint32(v Value, dst *uint32) error {
	switch vv := v.(type) {
	case uint32Value:
		*dst = uint32(vv)
		return nil
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to 'uint32' destination", v.Type().String())
	}
}

func castToInt32(v Value, dst *int32) error {
	switch vv := v.(type) {
	case int32Value:
		*dst = int32(vv)
		return nil
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to 'int32' destination", v.Type().String())
	}
}

func castToUint16(v Value, dst *uint16) error {
	switch vv := v.(type) {
	case uint16Value:
		*dst = uint16(vv)
		return nil
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to 'uint16' destination", v.Type().String())
	}
}

func castToInt16(v Value, dst *int16) error {
	switch vv := v.(type) {
	case int16Value:
		*dst = int16(vv)
		return nil
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to 'int16' destination", v.Type().String())
	}
}

func castToUint8(v Value, dst *uint8) error {
	switch vv := v.(type) {
	case uint8Value:
		*dst = uint8(vv)
		return nil
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to 'uint8' destination", v.Type().String())
	}
}

func castToInt8(v Value, dst *int8) error {
	switch vv := v.(type) {
	case int8Value:
		*dst = int8(vv)
		return nil
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to 'int8' destination", v.Type().String())
	}
}

func castToFloat64(v Value, dst *float64) error {
	switch vvv := v.(type) {
	case *doubleValue:
		*dst = vvv.value
		return nil
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to 'float64' destination", v.Type().String())
	}
}

func castToFloat32(v Value, dst *float32) error {
	switch vv := v.(type) {
	case *floatValue:
		*dst = vv.value
		return nil
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to 'float32' destination", v.Type().String())
	}
}
