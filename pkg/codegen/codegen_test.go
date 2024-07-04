package codegen

import (
	"fmt"
	"testing"
)

func TestCreateDagObject(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{"test1", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := GenData{DagDef: Dag{ID: ""}}
			if got, err := CreateDagGen(data, ""); false {
				if err != nil {
					t.Error()
				}
				fmt.Println(got)
				t.Errorf("CreateDagObject() = %v, want %v", got, tt.want)
			}
		})
	}
}
