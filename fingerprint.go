package cfilter

import (
	"bytes"
	"hash"
	"reflect"
)

type fingerprint []byte

func fprint(item []byte, fpSize uint8, hashfn hash.Hash) fingerprint {
	hashfn.Reset()
	hashfn.Write(item)
	h := hashfn.Sum(nil)

	fp := make(fingerprint, fpSize)
	copy(fp, h)

	return fp
}

func hashfp(f fingerprint) uint {
	var h uint = 5381
	for i := range f {
		h = ((h << 5) + h) + uint(f[i])
	}

	return h
}

func match(a, b fingerprint) bool {
	return bytes.Equal(a, b)
}

func (cf *CFilter) fingerprint(item []byte) fingerprint {
	if cf.hashPool.New != nil {
		if value := cf.hashPool.Get(); value != nil {
			hashfn := value.(hash.Hash)
			f := fprint(item, cf.fpSize, hashfn)
			cf.hashPool.Put(hashfn)
			return f
		}
	}

	cf.hashMu.Lock()
	defer cf.hashMu.Unlock()

	return fprint(item, cf.fpSize, cf.hashfn)
}

func (cf *CFilter) initHashPool() {
	newHash := hashClonerFactory(cf.hashfn)
	if newHash == nil || newHash() == nil {
		return
	}

	cf.hashPool.New = func() interface{} {
		cf.hashMu.Lock()
		hashfn := newHash()
		cf.hashMu.Unlock()

		if hashfn == nil {
			return nil
		}
		return hashfn
	}
}

func hashClonerFactory(hashfn hash.Hash) func() hash.Hash {
	clone := reflect.ValueOf(hashfn).MethodByName("Clone")
	if !clone.IsValid() {
		return nil
	}

	cloneType := clone.Type()
	if cloneType.NumIn() != 0 || cloneType.NumOut() == 0 {
		return nil
	}

	return func() hash.Hash {
		results := clone.Call(nil)
		if len(results) == 2 &&
			cloneType.Out(1) == reflect.TypeOf((*error)(nil)).Elem() &&
			!results[1].IsNil() {
			return nil
		}

		result := results[0]
		switch result.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
			reflect.Ptr, reflect.Slice:
			if result.IsNil() {
				return nil
			}
		}

		cloned, ok := result.Interface().(hash.Hash)
		if !ok || cloned == nil {
			return nil
		}
		return cloned
	}
}
