package gocouch

import (
  "testing";
)

var dbname string = "test123";

func clearDatabase(t *testing.T) {
  server := Server{"http://127.0.0.1:5984"};

  contains,err := server.Contains(dbname) ;

  if err != nil {
    t.Error(err.String());
  }  
  
  if contains {
    status, err := server.Delete(dbname);
    if err != nil || status == false {
      t.Error(err.String());
    }
  }
  
}

func initDatabase( t *testing.T) {
  clearDatabase(t);
  server := Server{"http://127.0.0.1:5984"};
  status, err := server.Create(dbname);
  if err != nil || status == false {
    t.Error(err.String());
  }  
}


func TestCreate(t *testing.T) {
  initDatabase(t);
}

func TestDelete(t *testing.T) {
  initDatabase(t);
  clearDatabase(t);
}
  
func TestGetAll(t *testing.T) {
  initDatabase(t);
  server := Server{"http://127.0.0.1:5984"};
  
  dbs, err := server.GetAll();
  
  if err != nil || dbs == nil {
    t.Error(err.String());
  } 
  
  if ! ( len( dbs) > 0) {
    t.Error( "Number of databases should be > 0" );
  }
}


func TestCreateDocument(t *testing.T) {
  initDatabase(t);
  test_document := struct { a int; b string } {12, "this is a test"};
  database := Database{"http://127.0.0.1:5984/test123/"};
  docid,err := database.Create(test_document);
  if err != nil {
    t.Error(err.String());
  }
  
  if docid == "" {
    t.Error("Doc Id is null");
  }
  
  var contents struct { A int; B string };
  
  if err := database.Get(docid, &contents); err != nil {
    t.Error(err.String());
  }
  
  if contents.A!= 12 || contents.B != "this is a test" {
    t.Error("parameter mismatch");
  }
  
}

func TestUpdateDocument(t *testing.T) {
  initDatabase(t);
  test_document := struct { a int; b string } {12, "this is a test"};
  database := Database{"http://127.0.0.1:5984/test123/"};
  docid,err := database.Create(test_document);
  if err != nil {
    t.Error(err.String());
  }
  
  if docid == "" {
    t.Error("Doc Id is null");
  }
  
  var contents struct { A int; B string };
  
  if err := database.Get(docid, &contents); err != nil {
    t.Error(err.String());
  }
  
  if contents.A!= 12 || contents.B != "this is a test" {
    t.Error("parameter mismatch");
  }
  
  contents.A= 100;
  contents.B= "this is a test 2";
  
  if err = database.Update(docid, contents); err != nil {
    t.Error(err.String());
  }
  
  if contents.A!= 100 || contents.B != "this is a test 2" {
    t.Error("parameter mismatch");
  }
}

func TestDeleteDocument(t *testing.T) {
  initDatabase(t);
  test_document := struct { a int; b string } {12, "this is a test"};
  database := Database{"http://127.0.0.1:5984/test123/"};
  docid,err := database.Create(test_document);
  if err != nil {
    t.Error(err.String());
  }
  
  if docid == "" {
    t.Error("Doc Id is null");
  }
  
  var contents struct { A int; B string };
  
  if err := database.Get(docid, &contents); err != nil {
    t.Error(err.String());
  }
  
  if err := database.Delete(docid); err != nil {
   t.Error(err.String());
  }
  
  contains,err := database.Contains(docid); 

  if err != nil {
   t.Error(err.String());
  }

  if contains {
    t.Error("Document should be deleted");
  }
  
}

type testdoc struct {
	name string; 
        gender string;
        age int;
        weight int;
}

var map_fun = `function(doc) {
     if (doc.gender == 'male')
        emit(doc.name, null);
}`

var test_docs = []testdoc {
  testdoc {"Charles", "male", 24, 198},
  testdoc {"Judy", "female", 15, 120},
  testdoc {"Philip", "male", 30, 154},
  testdoc {"Serena", "female", 45, 140},
  testdoc {"Gerry", "male", 24, 123},
}

func TestQueryDocument(t *testing.T) {
  initDatabase(t);
  database := Database{"http://127.0.0.1:5984/test123/"};
  
  for _, doc := range test_docs {
     _,err := database.Create(doc);
     if err != nil {
       t.Error(err.String());
     }
  }

  rows, err := database.Query(map_fun);
  if err != nil {
     t.Error(err.String());
  }
  if len(rows) != 3 {
     t.Error("Expected 3 rows");
  }

}
