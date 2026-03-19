package models

import "os"

// ProcessRegistrar tracks running OS processes per PR key so they can be
// killed when an autoplan is superseded by a newer commit.
type ProcessRegistrar interface {
	Register(prKey string, proc *os.Process)
	Deregister(prKey string, proc *os.Process)
}
