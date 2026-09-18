package base

import "testing"

func TestConvertOperationStringToConstant(t *testing.T) {
	op := ConvertOperationStringToConstant("create")
	if op != CreateOp {
		t.Fatalf("got %s, expected %s", op, CreateOp)
	}
	op = ConvertOperationStringToConstant("delete")
	if op != DeleteOp {
		t.Fatalf("got %s, expected %s", op, DeleteOp)
	}
	op = ConvertOperationStringToConstant("modify")
	if op != ModifyOp {
		t.Fatalf("got %s, expected %s", op, ModifyOp)
	}
	op = ConvertOperationStringToConstant("bind")
	if op != BindOp {
		t.Fatalf("got %s, expected %s", op, BindOp)
	}
	op = ConvertOperationStringToConstant("unbind")
	if op != UnBindOp {
		t.Fatalf("got %s, expected %s", op, UnBindOp)
	}
}
