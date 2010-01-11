package gocouch

import ( 
  "bytes";
  //"http";
  "container/vector"
  //"fmt"
  "httplib"
  "os";
  "io/ioutil";
  "json";
  "reflect";
  "strconv"
  "strings";
)

type Server struct {
  	res *resource;
}

func NewServer ( address string ) *Server {
	res := newResource ( address )
	server := Server { res }
	return &server
}

type Database struct {
	res *resource;
}

func NewDatabase ( address string ) *Database {
	res := newResource ( address )
	db := Database { res }
	return &db
}

type CouchError struct {
  Op string;  
  Message string; 
}

func (e CouchError) String() string {
	return e.Op + ":" + e.Message;
}

type resource struct {
	address string;
	client *httplib.Client;
}

func newResource ( address string ) *resource {
  if address[len(address)-1] == '/' {
    address = address[0:len(address)-1];
  }
  
  return &resource { address, &httplib.Client{} }
}

func (res *resource) buildURL(path ... ) string {  
  val := reflect.NewValue(path).(*reflect.StructValue);
  
  stripPaths := make([]string, val.NumField());
  
  for i:= 0; i < val.NumField(); i++ {
    s := val.Field(i).(*reflect.StringValue).Get();
    
    if len(s) == 0 {
    	continue;
    }
    
    if s[0] == '/' {
      s = s[1:];
    }
 	
	if len(s) == 0 {
		continue
	}
	
    if s[len(s)-1] == '/' {
      s = s[0:len(s)-1];
    }
    stripPaths[i] = s;
  }
  
  return res.address + "/" + strings.Join(stripPaths, "/");
}

func (res *resource) request(method string, path string, body string) (*httplib.Response, os.Error) {
 	headers := map[string]string{
		"Accept": "application/json",
  	};
  
  	if len(body) > 0 {
  		cl:= strconv.Itoa ( len (body) )
  		headers["Content-Length"] = cl
  	}
  	
  	url := res.buildURL ( path )
  
  	resp,err := res.client.Request ( url, method, headers, body )
  	
  	if err != nil {
  		println("error at request", err.String())
  		return nil,err
  	}
  	
  	return resp, nil
}

func readResponse ( resp *httplib.Response ) (string, os.Error) {
  data, err := ioutil.ReadAll(resp.Body);
  
  if err != nil {
    return "", err;
  }
  
  return string(data), nil;

}
func (res *resource) head(path string) (*httplib.Response , os.Error) {
  return res.request("HEAD", path, "");
}

func (res *resource) put(path string, body string) (*httplib.Response, os.Error) {
  return res.request("PUT", path, body);
}

func (res *resource) delete(path string) (*httplib.Response, os.Error) {
  return res.request("DELETE", path, "");
}

func (res *resource) get(path string) (*httplib.Response, os.Error) {
  return res.request("GET", path, "");
}

func (res *resource) post(path string, body string) (*httplib.Response, os.Error) {
  return res.request("POST", path, body);
}

func (server *Server) Contains(dbname string) (bool, os.Error) { 
  resp, err := server.res.head(dbname);
  
  if (err != nil) {
    return false, CouchError{"server.Contains", err.String()};
  }
  
  return (resp.Status == 200), nil;
}

func (server *Server) Create(dbname string) (b bool, err os.Error) { 
  resp, err := server.res.put(dbname,"");
  
  if (err != nil) {
    return false, CouchError{"server.Create", err.String()};
  }
  
  if (resp.Status == 412) {
    return false, CouchError{"server.Create", "database already exists"};
  }
  
  return true, nil;
}

func (server *Server) Delete(dbname string) (b bool, err os.Error) { 
  resp, err := server.res.delete(dbname);
  
  if (err != nil) {
    return false, CouchError{"server.Delete", err.String()};
  }
  
  if (resp.Status == 412) {
    return false, CouchError{"server.Delete", "database already exists"};
  }
  
  return true, nil;
}

func (server *Server) GetAll() ([]string, os.Error) { 
	resp, err := server.res.get("_all_dbs");
	if err != nil {
		return nil, CouchError{"server.GetAll", err.String()};
	}
  	
  	contents,err := readResponse( resp );
  	
  	if (err != nil) {
    	return nil, CouchError{"server.GetAll", err.String()};;
  	}
  	
	dbs := new(vector.StringVector)
  	json.Unmarshal ( contents, dbs)
  
  	return *dbs, nil;
}

func (server *Server) Len() (int, os.Error) { 
  dbs,err := server.GetAll();
  if (err != nil) {
    return -1, err;
  }
  
  return len(dbs), nil;
}

func (database *Database) Contains(docid string) (b bool, err os.Error) { 
  resp, err := database.res.head(docid);
  
  if err != nil {
    return false, err;
  }
  
  return (resp.Status == 200), nil;
}

func (database *Database) Get(docid string) (string, os.Error) { 
  resp, err := database.res.get(docid); 
  if err != nil {
    return "", err;
  }
  
  if resp.Status > 400 {
    return "", CouchError{"database.Get", "Not found"};
  }
  
  data,err := readResponse( resp ); 
  if err != nil {
    return "", err;
  }

  return data, nil;
}

func (database *Database) Create(data string) (string, string, os.Error) { 

  resp, err := database.res.post("/", data);
  
  if err != nil {
    return "","", err;
  }
  
  body,err := readResponse( resp );
  
  if err != nil || resp.Status > 400 {
  	return "", "", CouchError{"database.Create", "error"};
  }
  
  var couchResp struct { Ok string; Id string; Rev string }; 
  json.Unmarshal(body, &couchResp);

  return couchResp.Id, couchResp.Rev, nil;
}

func (database *Database) Update(id string, data string) os.Error { 
  resp, err := database.res.put(id, data);
  
  if err != nil {
    return err;
  }
  
  body,err := readResponse( resp );
  
  if err != nil || resp.Status > 400 {
    return CouchError{"database.Update", "error"};
  }
  var couchResp struct { Ok string; Id string; Rev string }; 

  json.Unmarshal(body, &couchResp);

  return nil;
}

func (database *Database) Delete(docid string, revid string) os.Error { 
/*
  	resp,err := database.res.head(docid);
  
  	if err != nil {
   		return err;
  	}
  	fmt.Printf("head results %v\n",resp.Headers)
  	etag := resp.Headers["Etag"][0]
  	rev := etag[1:len(etag)-1]
  	
  	if resp.Status == 200 {
  	*/
    	resp, err := database.res.delete(docid+"?rev="+revid);
    		
      	body,_ := readResponse( resp );
  	
  		println(body)

    	if err != nil || resp.Status > 400 {
    		return CouchError{"database.Delete", "error"};
    	}
 	//}
  	return nil;
}

type Row struct {
    Id string; 
    Key string; 
    Value string;
}

type QueryResults struct {
  Total_rows int;
  Offset int;
  Rows []Row;
}

func (database *Database) Query(map_fun string) ([] Row, os.Error) { 
  //var url string = buildURL(database.Address,"_temp_view");
  var body = map[string]string {"map": map_fun, "language":"javascript"};
  var buf bytes.Buffer;
  json.Marshal(&buf, body);
  resp, err := database.res.post("_temp_view", buf.String());
  if err != nil {
     return nil, err;
  }
  contents,err := readResponse(resp);
  var results QueryResults;
  json.Unmarshal(contents, &results);
  return results.Rows, nil
}

