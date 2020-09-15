package mongo

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"utilities-golang/logs"
	"utilities-golang/util"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	mgo "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type (
	FindCallback func(Cursor, error) error

	Mongo interface {
		AggregateWithContext(ctx context.Context,
			collection string, pipeline interface{}, callback FindCallback, options ...*options.AggregateOptions) error

		FindOneWithContext(context.Context, string, interface{}, interface{}, ...*options.FindOneOptions) error
		FindOne(string, interface{}, interface{}, ...*options.FindOneOptions) error

		FindAllWithContext(ctx context.Context, collection string, filter interface{}, results interface{}, options ...*options.FindOptions) error
		FindAll(collection string, filter interface{}, results interface{}, options ...*options.FindOptions) error

		FindWithContext(ctx context.Context,
			collection string, filter interface{}, callback FindCallback, options ...*options.FindOptions) error
		Find(string, interface{}, FindCallback, ...*options.FindOptions) error

		FindOneAndDeleteWithContext(context.Context, string, interface{}, ...*options.FindOneAndDeleteOptions) error
		FindOneAndDelete(string, interface{}, ...*options.FindOneAndDeleteOptions) error

		FindOneAndUpdateWithContext(context.Context, string, interface{}, interface{}, ...*options.FindOneAndUpdateOptions) error
		FindOneAndUpdate(string, interface{}, interface{}, ...*options.FindOneAndUpdateOptions) error

		InsertWithContext(context.Context, string, interface{}, ...*options.InsertOneOptions) (*primitive.ObjectID, error)
		Insert(string, interface{}, ...*options.InsertOneOptions) (*primitive.ObjectID, error)

		InsertManyWithContext(context.Context, string, []interface{}, ...*options.InsertManyOptions) ([]primitive.ObjectID, error)
		InsertMany(string, []interface{}, ...*options.InsertManyOptions) ([]primitive.ObjectID, error)

		UpdateWithContext(context.Context, string, interface{}, interface{}, ...*options.UpdateOptions) error
		Update(string, interface{}, interface{}, ...*options.UpdateOptions) error

		UpdateManyWithContext(context.Context, string, interface{}, interface{}, ...*options.UpdateOptions) error
		UpdateMany(string, interface{}, interface{}, ...*options.UpdateOptions) error

		DeleteManyWithContext(context.Context, string, interface{}, ...*options.DeleteOptions) error
		DeleteMany(string, interface{}, ...*options.DeleteOptions) error

		DeleteWithContext(context.Context, string, interface{}, ...*options.DeleteOptions) error
		Delete(string, interface{}, ...*options.DeleteOptions) error

		BulkDocumentWithContext(context.Context, string, []mgo.WriteModel) error
		BulkDocument(string, []mgo.WriteModel) error

		CountWithFilterAndContext(context.Context, string, interface{}, ...*options.CountOptions) (int64, error)
		CountWithFilter(string, interface{}, ...*options.CountOptions) (int64, error)
		CountWithContext(context.Context, string, ...*options.CountOptions) (int64, error)
		Count(string, ...*options.CountOptions) (int64, error)

		// - DDL
		Indexes(string) IndexView
		Client() *mgo.Client
		DB() Database
		util.Ping
	}

	implementation struct {
		client   *mgo.Client
		database Database
		logger   logs.Logger
	}

	decoder struct {
	}
)

func (d decoder) DecodeValue(dctx bsoncodec.DecodeContext, vr bsonrw.ValueReader, val reflect.Value) error {
	if !val.CanSet() || val.Kind() != reflect.String {
		return errors.New("bad type or not settable")
	}
	var str string
	var err error
	switch vr.Type() {
	case bsontype.String:
		if str, err = vr.ReadString(); err != nil {
			return err
		}
	case bsontype.Null: // THIS IS THE MISSING PIECE TO HANDLE NULL!
		if err = vr.ReadNull(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("cannot decode %v into a string type", vr.Type())
	}

	val.SetString(str)
	return nil
}

func (i *implementation) AggregateWithContext(ctx context.Context,
	collection string, pipeline interface{}, callback FindCallback, options ...*options.AggregateOptions) error {
	coll, err := i.database.Collection(collection).Aggregate(ctx, pipeline, options...)
	if err != nil {
		return err
	}

	cursor, err := NewCursor(coll)

	defer func() {
		if cursor == nil {
			return
		}

		if err := cursor.Close(ctx); err != nil {
			i.logger.Errorf("failed to close cursor %s", err)
		}
	}()

	if err != nil {
		return callback(nil, err)
	} else {
		return callback(cursor, nil)
	}
}

func New(ctx context.Context, uri, name string, logger logs.Logger) (Mongo, error) {
	if uri == "" {
		return nil, errors.New("uri is required!")
	}

	if name == "" {
		return nil, errors.New("database name is required!")
	}

	if logger == nil {
		return nil, errors.New("logger is required!")
	}

	opts := options.Client().
		ApplyURI(uri).
		SetRegistry(
			bson.NewRegistryBuilder().
				RegisterDecoder(reflect.TypeOf(""), decoder{}).
				Build(),
		)

	client, err := mgo.Connect(ctx, opts)

	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to mongo!")
	}

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, errors.Wrap(err, "failed to ping mongo")
	}

	database := NewDatabase(client.Database(name))

	return &implementation{client, database, logger}, nil
}

func (i *implementation) Ping() error {
	parentCtx := context.Background()
	ctx, cancel := context.WithTimeout(parentCtx, time.Second)
	defer cancel()
	return i.client.Ping(ctx, readpref.Primary())
}

func (i *implementation) Client() *mgo.Client {
	return i.client
}

func (i *implementation) DB() Database {
	return i.database
}

func (i *implementation) FindAllWithContext(ctx context.Context, collection string, filter interface{}, results interface{}, options ...*options.FindOptions) error {
	rs, err := i.database.Collection(collection).Find(ctx, filter, options...)

	if err != nil {
		return errors.Wrap(err, "failed to find all with context")
	}

	if err := rs.All(ctx, results); err != nil {
		return errors.Wrap(err, "failed to decode all")
	}

	return nil
}

func (i *implementation) FindAll(collection string, filter interface{}, results interface{}, options ...*options.FindOptions) error {
	return i.FindAllWithContext(context.Background(), collection, filter, results, options...)
}

func (i *implementation) FindOneWithContext(ctx context.Context, collection string, filter, object interface{}, options ...*options.FindOneOptions) error {
	sr := i.database.Collection(collection).FindOne(ctx, filter, options...)

	if err := sr.Err(); err != nil {
		return errors.Wrap(err, "FindOne failed!")
	}

	if err := sr.Decode(object); err != nil {
		return errors.Wrap(err, "FindOne decode failed!")
	}

	return nil
}

func (i *implementation) FindOne(collection string, filter interface{}, object interface{}, options ...*options.FindOneOptions) error {
	return i.FindOneWithContext(context.Background(), collection, filter, object, options...)
}

func (i *implementation) FindWithContext(ctx context.Context,
	collection string, filter interface{}, callback FindCallback, options ...*options.FindOptions) error {
	coll, err := i.database.Collection(collection).Find(ctx, filter, options...)
	if err != nil {
		return err
	}

	cursor, err := NewCursor(coll)

	defer func() {
		if cursor == nil {
			return
		}

		if err := cursor.Close(ctx); err != nil {
			i.logger.Errorf("failed to close cursor %s", err)
		}
	}()

	if err != nil {
		return callback(nil, err)
	} else {
		return callback(cursor, nil)
	}
}

func (i *implementation) Find(collection string, filter interface{}, callback FindCallback, options ...*options.FindOptions) error {
	return i.FindWithContext(context.Background(), collection, filter, callback, options...)
}

func (i *implementation) FindOneAndDeleteWithContext(ctx context.Context, collection string, filter interface{}, options ...*options.FindOneAndDeleteOptions) error {
	sr := i.database.Collection(collection).FindOneAndDelete(ctx, filter, options...)

	if err := sr.Err(); err != nil {
		return errors.Wrap(err, "FindOneAndDeleteWithContext failed!")
	}

	return nil
}

func (i *implementation) FindOneAndDelete(collection string, filter interface{}, options ...*options.FindOneAndDeleteOptions) error {
	return i.FindOneAndDeleteWithContext(context.Background(), collection, filter, options...)
}

func (i *implementation) FindOneAndUpdateWithContext(ctx context.Context, collection string, filter, object interface{}, options ...*options.FindOneAndUpdateOptions) error {
	sr := i.database.Collection(collection).FindOneAndUpdate(ctx, filter, object, options...)

	if err := sr.Err(); err != nil {
		return errors.Wrap(err, "FindOneAndUpdateWithContext failed!")
	}

	if err := sr.Decode(&object); err != nil {
		return errors.Wrap(err, "FindOneAndUpdate decode failed!")
	}

	return nil
}

func (i *implementation) FindOneAndUpdate(collection string, filter, object interface{}, options ...*options.FindOneAndUpdateOptions) error {
	return i.FindOneAndUpdateWithContext(context.Background(), collection, filter, object, options...)
}

func (i *implementation) InsertWithContext(ctx context.Context, collection string, object interface{}, options ...*options.InsertOneOptions) (*primitive.ObjectID, error) {
	ir, err := i.database.Collection(collection).InsertOne(ctx, object, options...)

	if err != nil {
		return nil, errors.Wrap(err, "InsertOneWithContext failed!")
	}

	id, ok := ir.InsertedID.(primitive.ObjectID)

	if !ok {
		return nil, errors.New("InsertWithContext failed to cast ObjectID")
	}

	return &id, nil
}

func (i *implementation) Insert(collection string, object interface{}, options ...*options.InsertOneOptions) (*primitive.ObjectID, error) {
	return i.InsertWithContext(context.Background(), collection, object, options...)
}

func (i *implementation) InsertManyWithContext(ctx context.Context, collection string, documents []interface{}, options ...*options.InsertManyOptions) ([]primitive.ObjectID, error) {
	ir, err := i.database.Collection(collection).InsertMany(ctx, documents, options...)

	if err != nil {
		return nil, errors.Wrap(err, "InsertManyWithContext failed!")
	}

	ids := make([]primitive.ObjectID, 0)

	for _, id := range ir.InsertedIDs {
		i, ok := id.(primitive.ObjectID)

		if !ok {
			err = errors.Errorf("InsertWithContext failed to cast ObjectID %s", i)
			break
		}

		ids = append(ids, i)
	}

	if err != nil {
		return nil, err
	}

	return ids, nil
}

func (i *implementation) InsertMany(collection string, documents []interface{}, options ...*options.InsertManyOptions) ([]primitive.ObjectID, error) {
	return i.InsertManyWithContext(context.Background(), collection, documents, options...)
}

func (i *implementation) UpdateWithContext(ctx context.Context, collection string, filter, object interface{}, options ...*options.UpdateOptions) error {
	if _, err := i.database.Collection(collection).UpdateOne(ctx, filter, object, options...); err != nil {
		return errors.Wrap(err, "UpdateWithContext failed!")
	}

	return nil
}

func (i *implementation) Update(collection string, filter, object interface{}, options ...*options.UpdateOptions) error {
	return i.UpdateWithContext(context.Background(), collection, filter, object, options...)
}

func (i *implementation) UpdateManyWithContext(ctx context.Context, collection string, filter, object interface{}, options ...*options.UpdateOptions) error {
	if _, err := i.database.Collection(collection).UpdateMany(ctx, filter, object, options...); err != nil {
		return errors.Wrap(err, "UpdateManyWithContext failed!")
	}

	return nil
}

func (i *implementation) UpdateMany(collection string, filter, object interface{}, options ...*options.UpdateOptions) error {
	return i.UpdateManyWithContext(context.Background(), collection, filter, object, options...)
}

func (i *implementation) DeleteManyWithContext(ctx context.Context, collection string, filter interface{}, options ...*options.DeleteOptions) error {
	if _, err := i.database.Collection(collection).DeleteMany(ctx, filter, options...); err != nil {
		return errors.Wrap(err, "DeleteManyWithContext failed!")
	}

	return nil
}

func (i *implementation) DeleteMany(collection string, filter interface{}, options ...*options.DeleteOptions) error {
	return i.DeleteManyWithContext(context.Background(), collection, filter, options...)
}

func (i *implementation) DeleteWithContext(ctx context.Context, collection string, filter interface{}, options ...*options.DeleteOptions) error {
	if _, err := i.database.Collection(collection).DeleteOne(ctx, filter, options...); err != nil {
		return errors.Wrap(err, "DeleteWithContext failed!")
	}

	return nil
}

func (i *implementation) CountWithFilterAndContext(ctx context.Context, collection string, filter interface{}, opts ...*options.CountOptions) (int64, error) {
	coll := i.database.Collection(collection)
	total, err := coll.CountDocuments(ctx, filter, opts...)

	if err != nil {
		return 0, errors.Wrapf(err, "count collection %s failed", collection)
	}

	return total, nil
}

func (i *implementation) CountWithFilter(collection string, filter interface{}, opts ...*options.CountOptions) (int64, error) {
	return i.CountWithFilterAndContext(context.Background(), collection, filter, opts...)
}

func (i *implementation) CountWithContext(ctx context.Context, collection string, opts ...*options.CountOptions) (int64, error) {
	return i.CountWithFilterAndContext(ctx, collection, bson.D{}, opts...)
}

func (i *implementation) Count(collection string, opts ...*options.CountOptions) (int64, error) {
	return i.CountWithContext(context.Background(), collection, opts...)
}

func (i *implementation) Delete(collection string, filter interface{}, options ...*options.DeleteOptions) error {
	return i.DeleteWithContext(context.Background(), collection, filter, options...)
}

func (i *implementation) Indexes(collection string) IndexView {
	return i.database.Collection(collection).Indexes()
}

func (i *implementation) BulkDocumentWithContext(ctx context.Context, collection string, data []mgo.WriteModel) error {
	_, err := i.database.Collection(collection).BulkWrite(ctx, data)
	if err != nil {
		return err
	}
	return nil
}

func (i *implementation) BulkDocument(collection string, data []mgo.WriteModel) error {
	return i.BulkDocumentWithContext(context.Background(), collection, data)
}

type (
	Cursor interface {
		Next(context.Context) bool
		Close(ctx context.Context) error
		Decode(val interface{}) error
	}

	cursorImplementation struct {
		cursor *mgo.Cursor
	}
)

func NewCursor(curr *mgo.Cursor) (Cursor, error) {
	return &cursorImplementation{cursor: curr}, nil
}

func (c *cursorImplementation) Next(ctx context.Context) bool {
	return c.cursor.Next(ctx)
}

func (c *cursorImplementation) Close(ctx context.Context) error {
	return c.cursor.Close(ctx)
}

func (c *cursorImplementation) Decode(val interface{}) error {
	return c.cursor.Decode(val)
}

type (
	Database interface {
		Collection(name string, opts ...*options.CollectionOptions) Collection
	}

	databaseimplementation struct {
		database *mgo.Database
	}
)

func NewDatabase(database *mgo.Database) Database {
	return &databaseimplementation{database: database}
}

func (d *databaseimplementation) Collection(name string, opts ...*options.CollectionOptions) Collection {
	return NewCollection(d.database.Collection(name, opts...))
}

type (
	Collection interface {
		Indexes() IndexView
		Aggregate(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) (*mgo.Cursor, error)
		Find(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (*mgo.Cursor, error)
		FindOne(ctx context.Context, filter interface{}, opts ...*options.FindOneOptions) *mgo.SingleResult
		BulkWrite(ctx context.Context, models []mgo.WriteModel, opts ...*options.BulkWriteOptions) (*mgo.BulkWriteResult, error)
		CountDocuments(ctx context.Context, filter interface{}, opts ...*options.CountOptions) (int64, error)
		DeleteOne(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mgo.DeleteResult, error)
		DeleteMany(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mgo.DeleteResult, error)
		UpdateMany(ctx context.Context, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (*mgo.UpdateResult, error)
		UpdateOne(ctx context.Context, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (*mgo.UpdateResult, error)
		InsertMany(ctx context.Context, documents []interface{}, opts ...*options.InsertManyOptions) (*mgo.InsertManyResult, error)
		InsertOne(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (*mgo.InsertOneResult, error)
		FindOneAndUpdate(ctx context.Context, filter interface{}, update interface{}, opts ...*options.FindOneAndUpdateOptions) *mgo.SingleResult
		FindOneAndDelete(ctx context.Context, filter interface{}, opts ...*options.FindOneAndDeleteOptions) *mgo.SingleResult
	}

	collectionimplementation struct {
		collection *mgo.Collection
	}
)

func NewCollection(collection *mgo.Collection) Collection {
	coll := collectionimplementation{collection: collection}
	return &coll
}

func (c *collectionimplementation) Indexes() IndexView {
	return NewIndexView(c.collection.Indexes())
}

func (c *collectionimplementation) Aggregate(ctx context.Context, pipeline interface{},
	opts ...*options.AggregateOptions) (*mgo.Cursor, error) {
	return c.collection.Aggregate(ctx, pipeline, opts...)
}

func (c *collectionimplementation) Find(ctx context.Context, filter interface{},
	opts ...*options.FindOptions) (*mgo.Cursor, error) {
	return c.collection.Find(ctx, filter, opts...)
}

func (c *collectionimplementation) FindOne(ctx context.Context, filter interface{},
	opts ...*options.FindOneOptions) *mgo.SingleResult {
	return c.collection.FindOne(ctx, filter, opts...)
}

func (c *collectionimplementation) BulkWrite(ctx context.Context, models []mgo.WriteModel,
	opts ...*options.BulkWriteOptions) (*mgo.BulkWriteResult, error) {
	return c.collection.BulkWrite(ctx, models, opts...)
}

func (c *collectionimplementation) CountDocuments(ctx context.Context, filter interface{},
	opts ...*options.CountOptions) (int64, error) {
	return c.collection.CountDocuments(ctx, filter, opts...)
}

func (c *collectionimplementation) DeleteOne(ctx context.Context, filter interface{},
	opts ...*options.DeleteOptions) (*mgo.DeleteResult, error) {
	return c.collection.DeleteOne(ctx, filter, opts...)
}

func (c *collectionimplementation) DeleteMany(ctx context.Context, filter interface{},
	opts ...*options.DeleteOptions) (*mgo.DeleteResult, error) {
	return c.collection.DeleteMany(ctx, filter, opts...)
}

func (c *collectionimplementation) UpdateMany(ctx context.Context, filter interface{}, update interface{},
	opts ...*options.UpdateOptions) (*mgo.UpdateResult, error) {
	return c.collection.UpdateMany(ctx, filter, update, opts...)
}

func (c *collectionimplementation) UpdateOne(ctx context.Context, filter interface{}, update interface{},
	opts ...*options.UpdateOptions) (*mgo.UpdateResult, error) {
	return c.collection.UpdateOne(ctx, filter, update, opts...)
}

func (c *collectionimplementation) InsertMany(ctx context.Context, documents []interface{},
	opts ...*options.InsertManyOptions) (*mgo.InsertManyResult, error) {
	return c.collection.InsertMany(ctx, documents, opts...)
}

func (c *collectionimplementation) InsertOne(ctx context.Context, document interface{},
	opts ...*options.InsertOneOptions) (*mgo.InsertOneResult, error) {
	return c.collection.InsertOne(ctx, document, opts...)
}

func (c *collectionimplementation) FindOneAndUpdate(ctx context.Context, filter interface{},
	update interface{}, opts ...*options.FindOneAndUpdateOptions) *mgo.SingleResult {
	return c.collection.FindOneAndUpdate(ctx, filter, update, opts...)
}

func (c *collectionimplementation) FindOneAndDelete(ctx context.Context, filter interface{},
	opts ...*options.FindOneAndDeleteOptions) *mgo.SingleResult {
	return c.collection.FindOneAndDelete(ctx, filter, opts...)
}

type (
	IndexView interface {
		List(ctx context.Context, opts ...*options.ListIndexesOptions) (Cursor, error)
		CreateMany(ctx context.Context, models []mgo.IndexModel, opts ...*options.CreateIndexesOptions) ([]string, error)
		CreateOne(ctx context.Context, model mgo.IndexModel, opts ...*options.CreateIndexesOptions) (string, error)
		DropOne(ctx context.Context, name string, opts ...*options.DropIndexesOptions) (bson.Raw, error)
		DropAll(ctx context.Context, opts ...*options.DropIndexesOptions) (bson.Raw, error)
	}

	indexviewimplementation struct {
		indexview mgo.IndexView
	}
)

func NewIndexView(indexview mgo.IndexView) IndexView {
	return &indexviewimplementation{indexview: indexview}
}

func (i *indexviewimplementation) List(ctx context.Context,
	opts ...*options.ListIndexesOptions) (Cursor, error) {
	return i.indexview.List(ctx, opts...)
}

func (i *indexviewimplementation) CreateMany(ctx context.Context, models []mgo.IndexModel,
	opts ...*options.CreateIndexesOptions) ([]string, error) {
	return i.indexview.CreateMany(ctx, models, opts...)
}

func (i *indexviewimplementation) CreateOne(ctx context.Context, model mgo.IndexModel,
	opts ...*options.CreateIndexesOptions) (string, error) {
	return i.indexview.CreateOne(ctx, model, opts...)
}

func (i *indexviewimplementation) DropOne(ctx context.Context, name string, opts ...*options.DropIndexesOptions) (bson.Raw, error) {
	return i.indexview.DropOne(ctx, name, opts...)
}

func (i *indexviewimplementation) DropAll(ctx context.Context, opts ...*options.DropIndexesOptions) (bson.Raw, error) {
	return i.indexview.DropAll(ctx, opts...)
}
