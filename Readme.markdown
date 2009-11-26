gocouch is a couchdb client for go. 

Installation
============

Because go's current http package cannot send generic http requests, gocouch requires a small change to the http to compile. Simply add the following method to src/pkg/http/client.go

    func Send(req *Request) (resp *Response, err os.Error) {
      return send(req);
    }


After that change, gocouch builds and installs like any other go package. Simply to make && make install to put it in the go binary package director. 

Usage
============

For usage examples, see gocouch_test.go. 


