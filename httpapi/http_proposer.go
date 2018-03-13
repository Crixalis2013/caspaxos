package httpapi

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/gorilla/mux"
	"github.com/peterbourgon/caspaxos/protocol"
)

// ProposerServer provides an HTTP interface to a proposer.
// It has a pretty restricted proposal API.
type ProposerServer struct {
	proposer protocol.Proposer
	*mux.Router
}

// NewProposerServer returns a usable ProposerServer wrapping the passed proposer.
func NewProposerServer(proposer protocol.Proposer) *ProposerServer {
	ps := &ProposerServer{
		proposer: proposer,
	}
	r := mux.NewRouter()
	{
		r.StrictSlash(true)
		r.Methods("POST").Path("/get/{key}").HandlerFunc(ps.handleGet)
		r.Methods("POST").Path("/cas/{key}").HandlerFunc(ps.handleCAS)
		r.Methods("POST").Path("/del/{key}").HandlerFunc(ps.handleDel)
		r.Methods("POST").Path("/add-accepter").HandlerFunc(ps.handleAddAccepter)
		r.Methods("POST").Path("/add-preparer").HandlerFunc(ps.handleAddPreparer)
		r.Methods("POST").Path("/remove-preparer").HandlerFunc(ps.handleRemovePreparer)
		r.Methods("POST").Path("/remove-accepter").HandlerFunc(ps.handleRemoveAccepter)
		r.Methods("POST").Path("/full-identity-read/{key}").HandlerFunc(ps.handleFullIdentityRead)
		r.Methods("POST").Path("/fast-forward-increment/{key}").HandlerFunc(ps.handleFastForwardIncrement)
	}
	ps.Router = r
	return ps
}

func (ps *ProposerServer) handleGet(w http.ResponseWriter, r *http.Request) {
	var (
		key  = mux.Vars(r)["key"]
		read = func(x []byte) []byte { return x }
	)

	state, b, err := ps.proposer.Propose(r.Context(), key, read)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	setBallot(w.Header(), b)
	w.Write(state)
}

func (ps *ProposerServer) handleCAS(w http.ResponseWriter, r *http.Request) {
	var (
		key        = mux.Vars(r)["key"]
		buf, _     = ioutil.ReadAll(r.Body)
		groups     = bytes.SplitN(buf, []byte{'\n', '\n'}, 2)
		curr, next = groups[0], groups[1]
	)
	cas := func(x []byte) []byte {
		if bytes.Equal(x, curr) {
			return next
		}
		return x
	}

	state, b, err := ps.proposer.Propose(r.Context(), key, cas)
	if _, ok := err.(protocol.ConflictError); ok {
		http.Error(w, err.Error(), http.StatusPreconditionFailed)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	setBallot(w.Header(), b)
	w.Write(state)
}

func (ps *ProposerServer) handleDel(w http.ResponseWriter, r *http.Request) {
	var (
		key     = mux.Vars(r)["key"]
		curr, _ = ioutil.ReadAll(r.Body)
		next    = []byte{}
	)
	cas := func(x []byte) []byte {
		if bytes.Equal(x, curr) {
			return next
		}
		return x
	}

	state, b, err := ps.proposer.Propose(r.Context(), key, cas)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !bytes.Equal(state, next) {
		http.Error(w, "conflict", http.StatusPreconditionFailed)
		return
	}

	setBallot(w.Header(), b)
	w.Write(state)
}

// AddAccepter(target Acceptor) error
func (ps *ProposerServer) handleAddAccepter(w http.ResponseWriter, r *http.Request) {
	buf, _ := ioutil.ReadAll(r.Body)
	u, err := url.Parse(string(buf))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	target := AcceptorClient{URL: u}
	if err := ps.proposer.AddAccepter(target); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintln(w, "OK")
}

// AddPreparer(target Acceptor) error
func (ps *ProposerServer) handleAddPreparer(w http.ResponseWriter, r *http.Request) {
	buf, _ := ioutil.ReadAll(r.Body)
	u, err := url.Parse(string(buf))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	target := AcceptorClient{URL: u}
	if err := ps.proposer.AddPreparer(target); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintln(w, "OK")
}

// RemovePreparer(target Acceptor) error
func (ps *ProposerServer) handleRemovePreparer(w http.ResponseWriter, r *http.Request) {
	buf, _ := ioutil.ReadAll(r.Body)
	u, err := url.Parse(string(buf))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	target := AcceptorClient{URL: u}
	if err := ps.proposer.RemovePreparer(target); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintln(w, "OK")
}

// RemoveAccepter(target Acceptor) error
func (ps *ProposerServer) handleRemoveAccepter(w http.ResponseWriter, r *http.Request) {
	buf, _ := ioutil.ReadAll(r.Body)
	u, err := url.Parse(string(buf))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	target := AcceptorClient{URL: u}
	if err := ps.proposer.RemoveAccepter(target); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintln(w, "OK")
}

// FullIdentityRead(ctx context.Context, key string) (state []byte, err error)
func (ps *ProposerServer) handleFullIdentityRead(w http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["key"]
	state, err := ps.proposer.FullIdentityRead(r.Context(), key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(state)
}

// FastForwardIncrement(ctx context.Context, key string, tombstone Ballot) (Age, error)
func (ps *ProposerServer) handleFastForwardIncrement(w http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["key"]
	tombstone := getBallot(r.Header)
	age, err := ps.proposer.FastForwardIncrement(r.Context(), key, tombstone)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	setAge(w.Header(), age)
	fmt.Fprintln(w, "OK")
}
