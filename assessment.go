package main

// import the necessary files
import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// crate a structure that would store data after reading it from csv
type Person struct {
	first_name  string
	last_name   string
	age         int64
	blood_group string
}

func main() {

	readCSVandPushToSQL()

}

func readCSVandPushToSQL() {

	// open the CSV file
	csvFile, err := os.Open("persons.csv")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("CSV file opened successfully!")

	// open the DB connection
	db, err := sql.Open("mysql", "root:1234@tcp(127.0.0.1:3306)/wanclouds")
	if err != nil {
		fmt.Println(err)
	}
	// don't close the connection
	defer db.Close()
	if err != nil {
		fmt.Println(err)
	}

	// don't close the CSV file until reading is complete
	defer csvFile.Close()

	// read the CSV file
	csvLines, err := csv.NewReader(csvFile).ReadAll()
	if err != nil {
		fmt.Println(err)
	}

	// create table in the database if it doesn't exist already
	query := "CREATE TABLE `wanclouds`.`person` (`first_name` VARCHAR(45) NULL,`last_name` VARCHAR(45) NULL,`age` INT NULL,`blood_group` VARCHAR(45) NULL)"

	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()

	_, error := db.ExecContext(ctx, query)
	if error != nil {
		fmt.Println(error)
	}

	var index int = 0
	// a for loop that reads CSV file line by line
	// stores it in a Person-type object, prints and then inserts into db
	for _, line := range csvLines {
		// convert the age into string to display
		age_of_person, _ := strconv.ParseInt(line[2], 10, 64)
		// make a new object person of each line of CSV
		pers := Person{
			first_name:  line[0],
			last_name:   line[1],
			age:         age_of_person,
			blood_group: line[3],
		}
		// Print the line
		//fmt.Println(pers.first_name + " " + pers.last_name + ", " + strconv.Itoa(int(pers.age)) + ", " + pers.blood_group)
		index = index + 1
		// Insert into MySql
		insert, err := db.Prepare("INSERT INTO person VALUES (?,?,?,?)")
		if err != nil {
			panic(err.Error())
		}
		insert.Exec(pers.first_name, pers.last_name, pers.age, pers.blood_group)

	}
	fmt.Println(strconv.Itoa(index) + " rows affected!")

}
