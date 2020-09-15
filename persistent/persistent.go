package persistent

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/jajotz/utilities-golang/logs"
	"github.com/jajotz/utilities-golang/util"

	"github.com/jinzhu/gorm"
	"github.com/pkg/errors"
)

const (
	UpsertQuery string = `insert into %s (%s) 
		values %s 
		on conflict (%s) 
			do update set %s`
	DeleteQuery        string = `delete from %s where %s`
	RawVarcharTemplate string = `%s%s%s`
	ExcludedQuery      string = ` "%s" = excluded."%s" `
)

type (
	Criteria struct {
		Field    string
		Operator string
		Value    interface{}
	}

	ORM interface {
		util.Ping
		Close() error

		Set(string, interface{}) ORM
		Error() error

		Where(interface{}, ...interface{}) ORM
		First(interface{}) error
		All(interface{}) error
		Order(interface{}) ORM
		Limit(interface{}) ORM
		Offset(interface{}) ORM

		Create(interface{}) error
		Update(interface{}) error
		Delete(interface{}) error
		BulkDelete(string, []interface{}) error
		SoftDelete(interface{}) error

		// Exec is used to execute sql Create, Update or Delete
		Exec(string, ...interface{}) error

		// RawSql is used to execute Select
		RawSqlWithObject(string, interface{}, ...interface{}) error
		RawSql(string, ...interface{}) (*sql.Rows, error)

		//Bulk Upsert
		BulkUpsert(string, int, []interface{}) error

		//Search
		Search(string, []string, []Criteria, interface{}) error

		HasTable(string) bool

		CreateTable(interface{}) error
		CreateTableWithName(string, interface{}) error

		DropTable(interface{}) error
		DropTableWithName(string, interface{}) error

		Table(string) ORM
		Begin() ORM
		Commit() error
		Rollback() error
	}

	Impl struct {
		Database *gorm.DB
		Err      error
		Logger   logs.Logger
	}

	Option struct {
		MaxIdleConnection, MaxOpenConnection int
		ConnMaxLifetime                      time.Duration
		LogMode                              bool
	}
)

func (o *Impl) Ping() error {
	return o.Database.DB().Ping()
}

func (o *Impl) Close() error {
	if err := o.Database.Close(); err != nil {
		return errors.Wrap(err, "failed to close database connection")
	}

	return nil
}

func (o *Impl) Set(key string, value interface{}) ORM {
	db := o.Database.Set(key, value)
	return &Impl{Database: db, Err: db.Error, Logger: o.Logger}
}

func (o *Impl) Error() error {
	return o.Err
}

func (o *Impl) Where(query interface{}, args ...interface{}) ORM {
	db := o.Database.Where(query, args...)
	return &Impl{Database: db, Err: db.Error, Logger: o.Logger}
}

func (o *Impl) First(object interface{}) error {
	db := o.Database.First(object)

	if err := db.Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return errors.Wrap(err, "failed to get first row")
		} else {
			return errors.Wrap(err, "")
		}
	}

	return nil
}

func (o *Impl) All(object interface{}) error {
	res := o.Database.Find(object)

	if err := res.Error; err != nil {
		return errors.Wrapf(err, "failed to query %s", object)
	}

	return nil
}

func (o *Impl) Order(args interface{}) ORM {
	db := o.Database.Order(args)
	return &Impl{Database: db, Err: db.Error, Logger: o.Logger}
}

func (o *Impl) Limit(args interface{}) ORM {
	db := o.Database.Limit(args)
	return &Impl{Database: db, Err: db.Error, Logger: o.Logger}
}

func (o *Impl) Offset(args interface{}) ORM {
	db := o.Database.Offset(args)
	return &Impl{Database: db, Err: db.Error, Logger: o.Logger}
}

func (o *Impl) Create(object interface{}) error {
	res := o.Database.Create(object)

	if err := res.Error; err != nil {
		return errors.Wrapf(err, "failed to create object %+v", object)
	}

	return nil
}

func (o *Impl) Update(object interface{}) error {
	res := o.Database.Save(object)

	if err := res.Error; err != nil {
		return errors.Wrapf(err, "failed to update object %+v", object)
	}

	return nil
}

func (o *Impl) Delete(object interface{}) error {
	res := o.Database.Unscoped().Delete(object)

	if err := res.Error; err != nil {
		return errors.Wrapf(err, "failed to delete object %+v", object)
	}

	return nil
}

func (o *Impl) BulkDelete(tableName string, bulkData []interface{}) error {

	if len(bulkData) == 0 {
		return errors.New("Bulk delete cannot empty")
	}

	deleteData := make([]map[string]interface{}, 0)

	var err error

	for i := 0; i < len(bulkData); i++ {
		data := bulkData[i]

		values := reflect.ValueOf(data)
		fields := reflect.TypeOf(data)

		fieldNum := reflect.TypeOf(data).NumField()

		temp := make(map[string]interface{})
		for i := 0; i < fieldNum; i++ {
			isPrimary := false
			name := fields.Field(i).Name
			if tag, ok := fields.Field(i).Tag.Lookup("gorm"); ok {
				tagParam := strings.Split(tag, ";")
				for _, param := range tagParam {
					paramMap := strings.Split(param, ":")
					if len(paramMap) == 2 {
						if paramMap[0] == "column" {
							name = paramMap[1]
						}
					} else if len(paramMap) == 1 {
						if paramMap[0] == "primary_key" {
							isPrimary = true
						}
					}
				}
			}

			if isPrimary {
				temp[name] = values.Field(i).Interface()
			}
		}

		deleteData = append(deleteData, temp)
	}

	if len(deleteData) > 0 {
		err = o.constructBulkDeleteQuery(tableName, deleteData)
		if err != nil {
			err = errors.Wrap(err, "error on bulk delete")
		}
	}

	return err
}

func (o *Impl) constructBulkDeleteQuery(tableName string, deleteData []map[string]interface{}) error {

	numericType := map[string]bool{
		"int8":       true,
		"uint8":      true,
		"int16":      true,
		"uint16":     true,
		"int32":      true,
		"uint32":     true,
		"int64":      true,
		"uint64":     true,
		"int":        true,
		"uint":       true,
		"uintptr":    true,
		"float32":    true,
		"float64":    true,
		"complex64":  true,
		"complex128": true,
		"bool":       true,
	}

	deleteValues := make([]string, 0)

	for _, row := range deleteData {
		data := "("

		fieldNum := len(row)
		countField := 0
		for name, value := range row {
			if name == "created_date" || name == "updated_date" {
				fieldNum = fieldNum - 1
				continue
			}

			data += `"` + name + `" = `

			typeName := reflect.TypeOf(value).Name()

			if _, ok := numericType[typeName]; ok {
				data += fmt.Sprintf("%v", value)
			} else {
				if typeName == "Time" {
					value = value.(time.Time).Format("2006-01-02 15:04:05.999999")
				} else if typeName == "Jsonb" {

					jsonBData, _ := json.Marshal(value)

					value = fmt.Sprintf("%v", strings.ReplaceAll(string(jsonBData), "'", "`"))
				}

				if value == nil {
					data += fmt.Sprintf("%v", value)
				} else {
					data += fmt.Sprintf("%s%v%s", `'`, value, `'`)
				}
			}

			if countField < fieldNum-1 {
				data += " and "
			}
			countField++
		}
		data += ")"

		deleteValues = append(deleteValues, data)
	}

	insertQuery := strings.Join(deleteValues, " or ")

	bulkQuery := fmt.Sprintf(DeleteQuery, tableName, insertQuery)

	err := o.Exec(bulkQuery)

	o.Commit()

	return err
}

func (o *Impl) SoftDelete(object interface{}) error {
	res := o.Database.Delete(object)

	if err := res.Error; err != nil {
		return errors.Wrapf(err, "failed to soft delete object %+v", object)
	}

	return nil
}

func (o *Impl) Begin() ORM {
	copied := o.Database.Begin()
	return &Impl{Database: copied, Err: copied.Error, Logger: o.Logger}

}

func (o *Impl) Rollback() error {
	res := o.Database.Rollback()

	if err := res.Error; err != nil {
		return errors.Wrap(err, "failed to rollback transaction!")
	}

	return nil
}

func (o *Impl) Commit() error {
	res := o.Database.Commit()

	if err := res.Error; err != nil {
		return errors.Wrap(err, "failed to commit transaction!")
	}

	return nil
}

func (o *Impl) Exec(sql string, args ...interface{}) error {
	res := o.Database.Exec(sql, args...)

	if err := res.Error; err != nil {
		return errors.Wrap(err, "failed to exec sql!")
	}

	return nil
}

func (o *Impl) RawSqlWithObject(sql string, object interface{}, args ...interface{}) error {
	res := o.Database.Raw(sql, args...).Scan(object)

	if err := res.Error; err != nil {
		return errors.Wrap(err, "failed to query sql!")
	}

	return nil
}

func (o *Impl) RawSql(sql string, args ...interface{}) (*sql.Rows, error) {
	return o.Database.Raw(sql, args...).Rows()
}

func (o *Impl) Table(tableName string) ORM {
	copied := o.Database.Table(tableName)
	return &Impl{Database: copied, Err: copied.Error, Logger: o.Logger}
}

func (o *Impl) Search(tableName string, selectField []string, criteria []Criteria, results interface{}) error {
	var (
		db = o.Database.Table(tableName)
	)

	if len(selectField) > 0 {
		db = db.Select(selectField)
	}

	for _, crit := range criteria {
		db = db.Where(crit.Field+" "+crit.Operator+" (?)", crit.Value)
	}

	res := db.Find(results)

	if err := res.Error; err != nil {
		return errors.Wrapf(err, "failed to query %s", results)
	}

	return nil
}

func (o *Impl) BulkUpsert(tableName string, chunkSize int, bulkData []interface{}) error {

	insertData := make([]map[string]interface{}, 0)

	fieldNames := make([]string, 0)
	primaryField := make([]string, 0)
	excludeField := make([]string, 0)
	shouldGetFieldName := true

	var err error

	for i := 0; i < len(bulkData); i++ {

		if len(insertData) >= chunkSize {
			//go func bulk upsert background

			itErr := o.constructBulkSearchQuery(tableName, fieldNames, primaryField, excludeField, insertData)
			if itErr != nil {
				if err == nil {
					err = itErr
				} else {
					err = errors.New(err.Error() + "/n" + itErr.Error())
				}
			}
			insertData = make([]map[string]interface{}, 0)
		}

		data := bulkData[i]

		values := reflect.ValueOf(data)
		fields := reflect.TypeOf(data)

		fieldNum := reflect.TypeOf(data).NumField()

		temp := make(map[string]interface{})
		for i := 0; i < fieldNum; i++ {
			name := fields.Field(i).Name
			isPrimary := false
			if tag, ok := fields.Field(i).Tag.Lookup("gorm"); ok {
				tagParam := strings.Split(tag, ";")
				for _, param := range tagParam {
					paramMap := strings.Split(param, ":")
					if len(paramMap) == 2 {
						if paramMap[0] == "column" {
							name = paramMap[1]
						}
					} else if len(paramMap) == 1 {
						if paramMap[0] == "primary_key" {
							isPrimary = true
						}
					}
				}
			}

			if shouldGetFieldName {
				fieldNames = append(fieldNames, name)
				if isPrimary {
					primaryField = append(primaryField, name)
				} else {
					excludeField = append(excludeField, name)
				}
			}
			temp[name] = values.Field(i).Interface()
		}

		if shouldGetFieldName {
			shouldGetFieldName = false
		}

		insertData = append(insertData, temp)
	}

	//bulk insert the rest
	if len(insertData) > 0 {
		itErr := o.constructBulkSearchQuery(tableName, fieldNames, primaryField, excludeField, insertData)
		if itErr != nil {
			if err == nil {
				err = itErr
			} else {
				err = errors.New(err.Error() + "/n" + itErr.Error())
			}
		}
	}

	return errors.Wrap(err, "error on bulk insert")
}

func (o *Impl) constructBulkSearchQuery(tableName string, fieldNames, primaryField, excludeField []string, data []map[string]interface{}) error {

	numericType := map[string]bool{
		"int8":       true,
		"uint8":      true,
		"int16":      true,
		"uint16":     true,
		"int32":      true,
		"uint32":     true,
		"int64":      true,
		"uint64":     true,
		"int":        true,
		"uint":       true,
		"uintptr":    true,
		"float32":    true,
		"float64":    true,
		"complex64":  true,
		"complex128": true,
		"bool":       true,
	}

	insertValues := make([]string, 0)

	fieldQuery := ""
	for k, name := range fieldNames {
		fieldQuery += fmt.Sprintf(RawVarcharTemplate, `"`, name, `"`)
		if k < len(fieldNames)-1 {
			fieldQuery += ", "
		}
	}

	primaryQuery := ""
	for k, name := range primaryField {
		primaryQuery += fmt.Sprintf(RawVarcharTemplate, `"`, name, `"`)
		if k < len(primaryField)-1 {
			primaryQuery += ", "
		}
	}

	conflictQuery := ""
	for k, name := range excludeField {
		conflictQuery += fmt.Sprintf(ExcludedQuery, name, name)
		if k < len(excludeField)-1 {
			conflictQuery += ", "
		}
	}

	for _, row := range data {

		data := "("

		fieldNum := len(row)
		countField := 0
		for _, value := range fieldNames {

			typeName := reflect.TypeOf(row[value]).Name()

			if _, ok := numericType[typeName]; ok {
				data += fmt.Sprintf("%v", row[value])
			} else {
				if typeName == "Time" {
					row[value] = row[value].(time.Time).Format("2006-01-02 15:04:05.999999")
				} else if typeName == "Jsonb" {

					jsonBData, _ := json.Marshal(row[value])

					row[value] = fmt.Sprintf("%v", strings.ReplaceAll(string(jsonBData), "'", "`"))
				}

				if row[value] == nil {
					data += fmt.Sprintf("%v", row[value])
				} else {
					data += fmt.Sprintf("%s%v%s", `'`, row[value], `'`)
				}
			}

			if countField < fieldNum-1 {
				data += ", "
			}
			countField++
		}
		data += ")"

		insertValues = append(insertValues, data)
	}

	insertQuery := strings.Join(insertValues, ", \n")

	bulkQuery := fmt.Sprintf(UpsertQuery, tableName, fieldQuery, insertQuery, primaryQuery, conflictQuery)

	err := o.Exec(bulkQuery)

	return err
}

func (o *Impl) CreateTable(data interface{}) error {
	return o.Database.CreateTable(data).Error
}

func (o *Impl) CreateTableWithName(tableName string, data interface{}) error {
	return o.Database.Table(tableName).CreateTable(data).Error
}

func (o *Impl) DropTable(data interface{}) error {
	return o.Database.DropTableIfExists(data).Error
}

func (o *Impl) DropTableWithName(tableName string, data interface{}) error {
	return o.Database.Table(tableName).DropTableIfExists(data).Error
}

func (o *Impl) HasTable(tableName string) bool {
	return o.Database.HasTable(tableName)
}
