package pbm

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
)

// Custom error when no snapshots are found
type NoSnapshotFoundError struct {
	Reason string
}

// Custom error when no snapshots are found
type OperationInProgressError struct {
	Reason string
}

func (c *NoSnapshotFoundError) Error() string {
	return c.Reason
}

func (c *OperationInProgressError) Error() string {
	return c.Reason
}

func LastPitrSnapshot(pbmClient *PBM) (*BackupMeta, error) {
	bcps, err := pbmClient.BackupsList(1) // Query only the latest snaspshot
	if err != nil {
		log.Fatalln("Error: unable to get snapshot list:", err)
		return nil, err
	}
	if len(bcps) == 0 {
		return nil, &NoSnapshotFoundError{Reason: "No snapshots found."}
	}
	return &bcps[0], nil
}

func RunBackup(pbmclient *PBM, bcpName, compression string) (string, error) {
	locks, err := pbmclient.GetLocks(&LockHeader{})
	if err != nil {
		log.Println("get locks", err)
	}

	ts, err := pbmclient.ClusterTime()
	if err != nil {
		return "", errors.Wrap(err, "read cluster time")
	}

	// Stop if there is some live operation.
	// But if there is some stale lock leave it for agents to deal with.
	for _, l := range locks {
		if l.Heartbeat.T+StaleFrameSec >= ts.T && l.Type != CmdPITR {
			return "", &OperationInProgressError{Reason: fmt.Sprintf("another operation in progress, %s/%s [%s/%s]", l.Type, l.OPID, l.Replset, l.Node)}
		}
	}

	cfg, err := pbmclient.GetConfig()
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return "", errors.New("no store set. Set remote store with <pbm store set>")
		}
		return "", errors.Wrap(err, "get remote-store")
	}

	err = pbmclient.SendCmd(Cmd{
		Cmd: CmdBackup,
		Backup: BackupCmd{
			Name:        bcpName,
			Compression: CompressionType(compression),
		},
	})
	if err != nil {
		return "", errors.Wrap(err, "send command")
	}

	ctx, cancel := context.WithTimeout(context.Background(), WaitBackupStart)
	defer cancel()
	err = waitForBcpStatus(ctx, pbmclient, bcpName)
	if err != nil {
		return "", err
	}

	storeString := ""
	switch cfg.Storage.Type {
	case StorageS3:
		storeString = "s3://"
		if cfg.Storage.S3.EndpointURL != "" {
			storeString += cfg.Storage.S3.EndpointURL + "/"
		}
		storeString += cfg.Storage.S3.Bucket
		if cfg.Storage.S3.Prefix != "" {
			storeString += "/" + cfg.Storage.S3.Prefix
		}
	case StorageFilesystem:
		storeString = cfg.Storage.Filesystem.Path
	}
	return storeString, nil
}

func waitForBcpStatus(ctx context.Context, pbmclient *PBM, bcpName string) error {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	var err error
	bmeta := new(BackupMeta)
	for {
		select {
		case <-tk.C:
			fmt.Print(".")
			bmeta, err = pbmclient.GetBackupMeta(bcpName)
			if err != nil {
				return errors.Wrap(err, "get backup metadata")
			}
			switch bmeta.Status {
			case StatusRunning, StatusDumpDone, StatusDone:
				return nil
			case StatusError:
				rs := ""
				for _, s := range bmeta.Replsets {
					rs += fmt.Sprintf("\n- Backup on replicaset \"%s\" in state: %v", s.Name, s.Status)
					if s.Error != "" {
						rs += ": " + s.Error
					}
				}
				return errors.New(bmeta.Error + rs)
			}
		case <-ctx.Done():
			rs := ""
			for _, s := range bmeta.Replsets {
				rs += fmt.Sprintf("- Backup on replicaset \"%s\" in state: %v\n", s.Name, s.Status)
				if s.Error != "" {
					rs += ": " + s.Error
				}
			}
			if rs == "" {
				rs = "<no replset has started backup>\n"
			}

			return errors.New("no confirmation that backup has successfully started. Replsets status:\n" + rs)
		}
	}
}
