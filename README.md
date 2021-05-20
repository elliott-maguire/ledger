# Ledger

    go get github.com/elliott-maguire/ledger

## Overview

This is an experiment in efficient auditing on two-dimensional datasets. It relies primarily on a semi-recursive change detection algorithm that produces a linear record of atomic changes for both rows and cells within a table. In the current iteration, this linear record is stored in a table adjacent to the "live" data (i.e. the most up-to-date copy).

## Project Status

Ledger still has a significant amount of issues and inefficiencies, and is not suitable for a production environment. The primary focus of development at this time is reducing memory overhead during table recomposition. I am also experimenting with implementations in other languages in an attempt to observe both advantages in the cleanliness of implementations and the overhead that other languages might incur/reduce.

## Architecture

### Database

The API requires `sqlx.DB` pointers in its function signatures so that the system can automatically maintain the following table structure:

1. A table for live/up-to-date data
2. A table for keeping the linear record
3. A table for caching inputs

### Input and Change Detection

Once new data comes in, ledger compares it to the existing data and tracks every change between the two datasets down to the cell. Those changes are then written to the `changes` table. The `live` table is then overwritten with the incoming data.

### Recomposition

Given a valid `time.Time` object, the `Recompose` will iterate over the change list in reverse and recompose the data, producing a copy of the data to the nearest accuracy.

## Usage

The only two functions that need to be called are `Update` and `Recompose`. These write new data and restore old data, respectively. All you need to have is a pointer to a `sqlx.DB` connection and some properly-formatted data.

```go
db, err := sqlx.Open("postgres", "postgresql://localhost:5432/mydb")
if err != nil { panic(err) }
defer db.Close()

data := map[string]interface{}{
    "1": map[string]interface{}{
        "name": "Jane Schmoe",
        "email": "jane@ledger.lib",
        "phone": "(123) 456-7890",
    },
    "2": map[string]interface{}{
        "name": "Joe Schmoe",
        "email": "joe@ledger.lib",
        "phone": "(098) 765-4321",
    },
}

if err := ledger.Update(db, "test", data); err != nil { panic(err) }
```

And to produce a previous version:

```go
restored, err := ledger.Recompose(); err != nil { panic(err) }
```
