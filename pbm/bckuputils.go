package pbm

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
)

func runBackup(cn *PBM, bcpName, compression string) (string, error) {
	locks, err := cn.GetLocks(&LockHeader{})
	if err != nil {
		log.Println("get locks", err)
	}

	ts, err := cn.ClusterTime()
	if err != nil {
		return "", errors.Wrap(err, "read cluster time")
	}

	// Stop if there is some live operation.
	// But if there is some stale lock leave it for agents to deal with.
	for _, l := range locks {
		if l.Heartbeat.T+StaleFrameSec >= ts.T && l.Type != CmdPITR {
			return "", errors.Errorf("another operation in progress, %s/%s [%s/%s]", l.Type, l.OPID, l.Replset, l.Node)
		}
	}

	cfg, err := cn.GetConfig()
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return "", errors.New("no store set. Set remote store with <pbm store set>")
		}
		return "", errors.Wrap(err, "get remote-store")
	}

	err = cn.SendCmd(Cmd{
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
	err = waitForBcpStatus(ctx, cn, bcpName)
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

func waitForBcpStatus(ctx context.Context, cn *PBM, bcpName string) error {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	var err error
	bmeta := new(BackupMeta)
	for {
		select {
		case <-tk.C:
			fmt.Print(".")
			bmeta, err = cn.GetBackupMeta(bcpName)
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
