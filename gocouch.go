package gocouch

import ( 
  "bytes";
  "http";
  "os";
  "io";
  "json";
  "reflect";
  "strings";
)

type Server struct {
  Address string;
}

type Database struct {
  Address string;
}

type CouchError struct {
  Op string;  
  Message string; 
}

func (e CouchError) String() string {
  return e.Op + ":" + e.Message;
}

func buildURL(base string, path ... ) string {  
  if base[len(base)-1] == '/' {
    base = base[0:len(base)-1];
  }
  val := reflect.NewValue(path).(*reflect.StructValue);
  
  stripPaths := make([]string, val.NumField());
  
  for i:= 0; i < val.NumField(); i++ {
    s := val.Field(i).(*reflect.StringValue).Get();
    if s[0] == '/' {
      s = s[1:];
    }
    
    if s[len(s)-1] == '/' {
      s = s[0:len(s)-1];
    }
    stripPaths[i] = s;
  }
  
  return base + "/" + strings.Join(stripPaths, "/");
}


func request(method string, url string, body io.Reader) (r *http.Response, err os.Error) {
  var req http.Request;
  req.Method = method;
  req.Header = map[string]string{
     "Accept": "application/json",
  };
  req.Body = body;
  
  req.URL, err = http.ParseURL(url);
  
  if err != nil {
    return nil, err
  }
  
  return http.Send(&req);
}

func readResponse ( response *http.Response ) (content string, err os.Error) {
  var b [] byte;
  b, err = io.ReadAll(response.Body);
  response.Body.Close();
  if err != nil {
    return "", err;
  }
  return string(b), nil;
}

func head(url string) (r *http.Response, err os.Error) {
  return request("HEAD", url, nil);
}

func put(url string) (r *http.Response, err os.Error) {
  return request("PUT", url, nil);
}

func delete(url string) (r *http.Response, err os.Error) {
  return request("DELETE", url, nil);
}

func get(url string) (r *http.Response, err os.Error) {
  return request("GET", url, nil);
}

func post(url string, body string) (r *http.Response, err os.Error) {
  return request("POST", url, bytes.NewBufferString(body));
}

func (server *Server) Contains(dbname string) (b bool, err os.Error) { 
  var url string = server.Address + "/" + dbname;
  resp, err := head(url);
  
  if (err != nil)
  {
    return false, err;
  }
  
  return (resp.StatusCode == 200), nil;
}

func (server *Server) Create(dbname string) (b bool, err os.Error) { 
  var url string = server.Address + "/" + dbname;
  resp, err := put(url);
  
  if (err != nil)
  {
    return false, CouchError{"server.create", err.String()};
  }
  
  if (resp.StatusCode == 412)
  {
    return false, CouchError{"server.create", "database already exists"};
  }
  return true, nil;
}

func (server *Server) Delete(dbname string) (b bool, err os.Error) { 
  var url string = server.Address + "/" + dbname;
  resp, err := delete(url);
  
  if (err != nil)
  {
    return false, CouchError{"server.delete", err.String()};
  }
  
  if (resp.StatusCode == 412)
  {
    return false, CouchError{"server.delete", "database already exists"};
  }
  return true, nil;
}

func (server *Server) GetAll() ([] string, os.Error) { 
  var url string = server.Address + "/" + "_all_dbs";
  resp, _, err := http.Get(url);
  if err != nil {
    return nil, err;
  }
  
  contents,err := readResponse( resp );
  
  if (err != nil)
  {
    return nil, err;
  }
  
  mapv, ok, _ := json.StringToJson(contents);
  
  if !ok {
    return nil, CouchError{"server.GetAll", "error parsing sever response"};
  }
  
  var dbs []string = make([]string, mapv.Len());
  
  for i := 0; i < mapv.Len(); i++ {
    dbs[i] = mapv.Elem(i).String();
  }

  return dbs, nil;
}

func (server *Server) Len() (int, os.Error) { 
  dbs,err := server.GetAll();
  if (err != nil) {
    return -1, err;
  }
  
  return len(dbs), nil;
}

func (database *Database) Contains(docid string) (b bool, err os.Error) { 
  var url string = database.Address + "/" + docid;
  resp, err := head(url);
  
  if err != nil {
    return false, err;
  }
  
  return (resp.StatusCode == 200), nil;
}

func (database *Database) GetJson(docid string) (string, os.Error) { 
  var url string = database.Address+"/"+docid;
  

  resp, err := get(url); 
  if err != nil {
    return "", err;
  }
  
  if resp.StatusCode > 400 {
    return "", CouchError{"database.Get", "error"};
  }
  
  data,err := readResponse( resp ); 
  if err != nil {
    return "", err;
  }

  return data, nil;
}


func (database *Database) Get(docid string, val interface{}) os.Error { 
  body,err := database.GetJson(docid);

  if err != nil {
    return err;
  }
  
  ok, errtok := json.Unmarshal(body, val ); 
  
  if !ok {
    return CouchError{"database.Get", errtok};;
  }
  
  return nil;
}


func (database *Database) Create(contents interface{}) (string, os.Error) { 
  var url string = database.Address;
  var buf bytes.Buffer;
  
  err := json.Marshal(&buf, contents );
  
  resp, err := post(url, string(buf.Bytes()));
  
  if err != nil {
    return "", err;
  }
  
  
  data,err := readResponse( resp );
  if err != nil || resp.StatusCode > 400 {
    
  }
  var couchResp struct { Ok string; Id string; Rev string }; 

  json.Unmarshal(data, &couchResp);

  return couchResp.Id, nil;
}

func (database *Database) Update(id string, contents interface{}) os.Error { 
  var url string = database.Address + "/" + id;
  var buf bytes.Buffer;
  
  err := json.Marshal(&buf, contents );
  
  resp, err := post(url, string(buf.Bytes()));
  
  if err != nil {
    return err;
  }
  
  
  data,err := readResponse( resp );
  if err != nil || resp.StatusCode > 400 {
    
  }
  var couchResp struct { Ok string; Id string; Rev string }; 

  json.Unmarshal(data, &couchResp);

  return nil;
}

func (database *Database) Delete(docid string) os.Error { 
  var url string = buildURL(database.Address, docid);
  resp,err := head(url);
  
  if err != nil {
     return err;
  }
  rev := resp.Header["Etag"][1:len(resp.Header["Etag"])-1];
  if resp.StatusCode == 200 {
    url += "?rev="+rev;
    resp, err = delete(url);
  }
  
  return nil;
}

func (database *Database) Query(map_fun string) os.Error { 
   var url string = buildURL(database.Address,"_temp_view");
   var body = map[string]string {"map": map_fun, "language":"javascript"};

  /*
   var buf bytes.Buffer;
   err := json.Marshal(&buf, body);
   if err != nil {
       return nil;
   }
   println(buf.String());
   */
   return nil;
}


