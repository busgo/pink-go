package client

type Job interface {
	Target() string
	// execute job
	Execute(param string) (string, error)
}
