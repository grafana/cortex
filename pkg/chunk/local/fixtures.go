package local

import (
	"io/ioutil"
	"os"
	"time"

	"github.com/prometheus/common/model"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/testutils"
)

type fixture struct {
	name    string
	dirname string
}

func (f *fixture) Name() string {
	return f.name
}

func (f *fixture) Clients() (
	indexClient chunk.IndexClient, chunkClient chunk.ObjectClient, tableClient chunk.TableClient,
	schemaConfig chunk.SchemaConfig, err error,
) {
	f.dirname, err = ioutil.TempDir(os.TempDir(), "boltdb")
	if err != nil {
		return
	}

	indexClient = NewBoltDBIndexClient(BoltDBConfig{
		Directory: f.dirname,
	})

	chunkClient = NewFSObjectClient(FSConfig{
		Directory: f.dirname,
	})

	tableClient, err = NewTableClient()
	if err != nil {
		return
	}

	schemaConfig = chunk.SchemaConfig{
		Configs: []chunk.PeriodConfig{{
			IndexType: "boltdb",
			From:      model.Now(),
			ChunkTables: chunk.PeriodicTableConfig{
				Prefix: "chunks",
				Period: 10 * time.Minute,
			},
		}},
	}

	return
}

func (f *fixture) Teardown() error {
	return os.RemoveAll(f.dirname)
}

// Fixtures for unit testing GCP storage.
var Fixtures = []testutils.Fixture{
	&fixture{
		name: "boltdb",
	},
}
