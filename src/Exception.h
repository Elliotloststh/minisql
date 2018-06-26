#ifndef Exception
#define Exception

#include <iostream>
#include <exception>
#include <string>

using namespace std;

class CatalogError:public exception
{
public:
    CatalogError(string s)
    {
        error = "Catalog Error: " + s;
    }
    virtual const char *what()const throw()
    {
        return error.c_str();
    }
private:
    string error;
};

#endif
