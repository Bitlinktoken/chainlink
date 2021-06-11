package feeds

import (
	"context"
	"crypto/ed25519"
	"errors"
	"log"
	"sync"

	"github.com/smartcontractkit/chainlink/core/logger"
	pb "github.com/smartcontractkit/chainlink/core/services/feeds/proto"
	"github.com/smartcontractkit/chainlink/core/services/keystore"
	"github.com/smartcontractkit/wsrpc"
)

//go:generate mockery --name Service --output ./mocks/ --case=underscore

type Service interface {
	Start() error
	Close() error

	CountManagers() (int64, error)
	CreateJobProposal(jp *JobProposal) (uint, error)
	GetManager(id int32) (*FeedsManager, error)
	ListManagers() ([]FeedsManager, error)
	RegisterManager(ms *FeedsManager) (int32, error)
}

type service struct {
	mu     sync.Mutex
	chDone chan struct{}

	orm       ORM
	ks        keystore.CSAKeystoreInterface
	fmsClient pb.FeedsManagerClient
}

// NewService constructs a new feeds service
func NewService(orm ORM, ks keystore.CSAKeystoreInterface) Service {
	svc := &service{
		chDone: make(chan struct{}),
		orm:    orm,
		ks:     ks,
	}

	return svc
}

// RegisterManager registers a new ManagerService and attempts to establish a
// connection.
//
// Only a single feeds manager is currently supported.
func (s *service) RegisterManager(fm *FeedsManager) (int32, error) {
	count, err := s.CountManagers()
	if err != nil {
		return 0, err
	}
	if count >= 1 {
		return 0, errors.New("only a single feeds manager is supported")
	}

	id, err := s.orm.CreateManager(context.Background(), fm)
	if err != nil {
		return 0, err
	}

	// Establish a connection
	err = s.Start()
	if err != nil {
		logger.Infof("Could not establish a connection: %v", err)
	}

	return id, nil
}

// ListManagerServices lists all the manager services.
func (s *service) ListManagers() ([]FeedsManager, error) {
	return s.orm.ListManagers(context.Background())
}

// GetManager gets a manager service by id.
func (s *service) GetManager(id int32) (*FeedsManager, error) {
	return s.orm.GetManager(context.Background(), id)
}

// CountManagerServices gets the total number of manager services
func (s *service) CountManagers() (int64, error) {
	return s.orm.CountManagers()
}

func (s *service) CreateJobProposal(jp *JobProposal) (uint, error) {
	return s.orm.CreateJobProposal(context.Background(), jp)
}

func (s *service) Start() error {
	// Fetch the server's public key
	keys, err := s.ks.ListCSAKeys()
	if err != nil {
		return err
	}
	if len(keys) < 1 {
		return errors.New("CSA key does not exist")
	}

	privkey, err := s.ks.Unsafe_GetUnlockedPrivateKey(keys[0].PublicKey)
	if err != nil {
		return err
	}

	// We only support a single feeds manager right now
	mgrs, err := s.ListManagers()
	if err != nil {
		return err
	}
	if len(mgrs) < 1 {
		return errors.New("no feeds managers registered")
	}

	mgr := mgrs[0]

	go s.connect(mgr.URI, privkey, mgr.PublicKey, mgr.ID)

	return nil
}

// Connect attempts to establish a connection to the Feeds Manager.
//
// In the future we will connect to multiple Feeds Managers
func (s *service) connect(uri string, privkey []byte, pubkey []byte, feedsManagerID int32) error {
	conn, err := wsrpc.Dial(uri,
		wsrpc.WithTransportCreds(privkey, ed25519.PublicKey(pubkey)),
	)
	if err != nil {
		log.Fatalln(err)
	}

	// Initialize a new wsrpc client to make RPC calls
	s.mu.Lock()
	s.fmsClient = pb.NewFeedsManagerClient(conn)
	s.mu.Unlock()
	defer conn.Close()

	// Initialize RPC call handlers on the client connection
	pb.RegisterNodeServiceServer(conn, &RPCHandlers{
		feedsManagerID: feedsManagerID,
		svc:            s,
	})

	// Wait for close
	<-s.chDone

	return nil
}

func (s *service) Close() error {
	close(s.chDone)
	return nil
}
