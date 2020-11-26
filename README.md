# bricks

Bricks is a tool for storing and keeping histories of flat, two-dimensional data sets. It maintains a linear, atomic, granular change history that can be traversed to reconstruct any available past version of the given data set. Changes in data are tracked down to the cell level allowing for flexibility with constantly-changing schema.

    go get github.com/sr-revops/bricks

## What would this be used for?

The tool was developed out of our need to warehouse Salesforce report data in an efficient manner. It can be used on any input as long as it's in the right format, but any flat data sets that are constantly evolving are excellent use cases.

## How does it work?

### Database Architecture

Bricks automatically maintains a database architecture for the data that comes in. It is required that a label for the data be passed, and that label is used to create a schema under which three tables exist: `live`, where the most up-to-date copy of the data lives, `changes`, where the change list lives, and `archive` where restored data is written temporarily.

### Input and Change Detection 

Once new data comes in, Bricks compares it to the existing data and tracks every change between the two datasets down to the cell. Those changes are then written to the `changes` table. The `live` table is then overwritten with the incoming data.

### Restoration

Given a valid `time.Time` object, Bricks will iterate over the change list in reverse and re-create the data as it stood at the given time. It will write that restored data set to the `archive` table, and return a copy of the data.

## How is it used?

The only two functions that need to be called are `Update` and `Restore`. These write new data and restore old data, respectively. All you need to have is a pointer to a `sqlx.DB` connection and some properly-formatted data.

```go
db, err := sqlx.Open("postgres", "postgresql://localhost:5432/mydb")
if err != nil { panic(err) }
defer db.Close()

data := map[string]interface{}{
    "1": map[string]interface{}{
        "name": "Jane Schmoe",
        "email": "jane@bricks.lib",
        "phone": "(123) 456-7890",
    },
    "2": map[string]interface{}{
        "name": "Joe Schmoe",
        "email": "joe@bricks.lib",
        "phone": "(098) 765-4321",
    },
}

if err := bricks.Update(db, "testupdate", data); err != nil { panic(err) }
```

And if the data were to change:

```go
restored, err := bricks.Restore(); err != nil { panic(err) }
```